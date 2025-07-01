#!/usr/bin/env python3
"""
Bulk off-loader: JDBC → Iceberg on MinIO/S3.
Adds per-row retention metadata columns and prints a summary.
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Tuple

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from offloadlib.config import load


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _parse_iso_duration(text: str) -> timedelta:
    """
    Accepts:
      • Full ISO-8601 durations   e.g.  P3Y  P6M  P45D
      • Shorthand like 6m / 90d / 2y
    """
    # normal ISO-8601 Y/M/D
    iso = re.match(r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?$", text, re.I)
    if iso:
        y, m, d = (int(x or 0) for x in iso.groups())
        return timedelta(days=y * 365 + m * 30 + d)

    # very short forms: 90d, 6m, 2y   (case-insensitive)
    short = re.match(r"^(\d+)([dmy])$", text, re.I)
    if short:
        num, unit = int(short[1]), short[2].lower()
        factor = {"d": 1, "m": 30, "y": 365}[unit]
        return timedelta(days=num * factor)

    raise ValueError(f"Unsupported duration literal: {text!r}")


def spark_session(env: str, warehouse: str) -> SparkSession:
    """Configure Spark + Iceberg + MinIO (pure-local mode)."""
    builder = (
        SparkSession.builder.appName("bulk-offload")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", f"{warehouse}/{env}")
        .config("spark.sql.catalog.default", "local")
        .config("spark.sql.adaptive.enabled", "true")
    )

    endpoint = os.getenv("S3_ENDPOINT")  # e.g. http://minio:9000
    if endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    return builder.getOrCreate()


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="conf/offload.yaml")
    ap.add_argument("--env", default=os.getenv("GLUE_ENV", "dev"))
    args = ap.parse_args()

    cfg = load(args.config)
    spark = spark_session(cfg.env, cfg.warehouse)

    # Pre-compute retention policy → timedelta
    retention_periods: Dict[str, timedelta] = {
        name: _parse_iso_duration(value)
        for name, value in cfg.retention_policies.items()
    }

    # Statistics
    # full_table_name -> (n_rows, n_cols)
    written: Dict[str, Tuple[int, int]] = {}

    now_ts = datetime.utcnow()

    for src in cfg.sources:
        policy_td = retention_periods[src.retention_policy]
        conn = cfg.connections[src.connection]
        user, pwd = conn.creds()
        if not user or not pwd:
            raise RuntimeError(
                f"Missing creds for connection {src.connection}")

        for schema in src.include["schemas"]:
            db_name = f"{cfg.env}_{schema}"
            for pattern in src.include["tables"]:
                # --- discover candidate tables
                tbls_df: DataFrame = (
                    spark.read.format("jdbc")
                    .option("url", conn.jdbc_url)
                    .option(
                        "dbtable",
                        f"(SELECT table_name FROM information_schema.tables "
                        f"WHERE table_schema='{schema}') AS t",
                    )
                    .option("user", user)
                    .option("password", pwd)
                    .option("driver", conn.driver)
                    .load()
                )
                candidates = [
                    r["table_name"]
                    for r in tbls_df.collect()
                    if re.match(pattern, r["table_name"], re.IGNORECASE)
                ]

                for tbl in candidates:
                    ice_tbl = re.sub(r"[^a-z0-9_]", "_", tbl.lower())
                    full = f"local.{db_name}.{ice_tbl}"
                    spark.sql(f"DROP TABLE IF EXISTS {full} PURGE")

                    base_df: DataFrame = (
                        spark.read.format("jdbc")
                        .option("url", conn.jdbc_url)
                        .option("dbtable", f"{schema}.{tbl}")
                        .option("user", user)
                        .option("password", pwd)
                        .option("driver", conn.driver)
                        .load()
                    )

                    # --- add retention columns
                    df = (
                        base_df
                        .withColumn("retention_added_at", F.lit(now_ts))
                        .withColumn(
                            "retention_expires_at",
                            F.lit(now_ts + policy_td),
                        )
                    )

                    (
                        df.write.format("iceberg")
                        .mode("overwrite")
                        .option("path", f"{cfg.warehouse}/{cfg.env}/{db_name}/{ice_tbl}")
                        .saveAsTable(full)
                    )

                    n_rows = df.count()
                    written[full] = (n_rows, len(df.columns))
                    print(f"✓ {schema}.{tbl} → {full} ({n_rows} rows)")

    spark.stop()

    # ── summary ──────────────────────────────────────────────
    if written:
        print("\nSummary")
        print("-------")
        total_rows = sum(r for r, _ in written.values())
        for table, (rows, cols) in written.items():
            print(f"{table:<50} {rows:>10} rows   {cols:>3} cols")
        print(f"\nTotal tables: {len(written)}   Total rows: {total_rows}")


if __name__ == "__main__":
    main()
