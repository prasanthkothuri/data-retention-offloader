#!/usr/bin/env python3
"""
Bulk off-loader: JDBC → Iceberg on MinIO/S3 (local) or AWS Glue.
Adds per-row retention metadata columns and prints a summary.
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from offloadlib.config import load, get_glue_creds


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def _parse_iso_duration(text: str) -> timedelta:
    """Handle full ISO-8601 dates (P6M) and short forms (6m / 90d / 2y)."""
    iso = re.match(r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?$", text, re.I)
    if iso:
        y, m, d = (int(x or 0) for x in iso.groups())
        return timedelta(days=y * 365 + m * 30 + d)

    short = re.match(r"^(\d+)([dmy])$", text, re.I)
    if short:
        num, unit = int(short[1]), short[2].lower()
        return timedelta(days=num * {"d": 1, "m": 30, "y": 365}[unit])

    raise ValueError(f"Unsupported duration literal: {text!r}")


def spark_session(env: str, warehouse: str, mode: str) -> SparkSession:
    """Return a SparkSession configured for either local MinIO or AWS Glue."""
    builder = SparkSession.builder.appName("bulk-offload")

    if mode == "local":
        builder = (
            builder
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", f"{warehouse}/{env}")
            .config("spark.sql.catalog.default", "local")
            .config("spark.sql.adaptive.enabled", "true")
        )

        # Optional MinIO endpoint
        endpoint = os.getenv("S3_ENDPOINT")          # e.g. http://minio:9000
        if endpoint:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", endpoint)
                .config("spark.hadoop.fs.s3a.access.key",
                        os.getenv("AWS_ACCESS_KEY_ID", "minio"))
                .config("spark.hadoop.fs.s3a.secret.key",
                        os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            )
    else:   # ── GLUE MODE ─────────────────────────────────────
        s3_region = "eu-west-2"
        builder = (
            builder
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
            .config("spark.sql.catalog.default", "glue_catalog")
            .config("spark.sql.adaptive.enabled", "true")
            # Critical regional configuration
            .config("spark.hadoop.fs.s3a.region", s3_region)
            .config("spark.sql.catalog.glue_catalog.glue.region", s3_region)
            # Optional but recommended
            .config("spark.hadoop.fs.s3a.endpoint.region", s3_region)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")

        )

    return builder.getOrCreate()


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="conf/offload.yaml")
    ap.add_argument("--env", default=os.getenv("GLUE_ENV", "dev"))
    ap.add_argument("--mode", choices=["local", "glue"], default="local",
                    help="Execution mode: local (default) or glue")
    args, _ = ap.parse_known_args()

    cfg = load(args.config)
    spark = spark_session(cfg.env, cfg.warehouse, args.mode)
    catalog = "glue_catalog" if args.mode == "glue" else "local"

    # Pre-compute retention policy → timedelta
    retention_periods: Dict[str, timedelta] = {
        name: _parse_iso_duration(value)
        for name, value in cfg.retention_policies.items()
    }

    written: Dict[str, Tuple[int, int]] = {}
    now_ts = datetime.utcnow()

    for src in cfg.sources:
        conn = cfg.connections[src.connection]
        policy_td = retention_periods[src.retention_policy]

        # 1. try env-vars first (works for local mode)
        user, pwd = conn.creds()

        # 2. if we’re running in Glue AND the env-vars are empty,
        #    pull them from the Glue connection we just created
        if args.mode == "glue" and (not user or not pwd):
            user, pwd, conn.jdbc_url = get_glue_creds(src.connection)

        # 3. still missing?  hard-fail
        if not user or not pwd:
            raise RuntimeError(
                f"Missing creds for connection {src.connection}"
            )

        for schema in src.include["schemas"]:
            db_name = f"{cfg.env}_{schema}"
            for pattern in src.include["tables"]:
                # discover candidate tables
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
                    full = f"{catalog}.{db_name}.{ice_tbl}"

                    # DROP only makes sense in local dev
                    if args.mode == "local":
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

                    df = (
                        base_df
                        .withColumn("retention_added_at", F.lit(now_ts))
                        .withColumn("retention_expires_at",
                                    F.lit(now_ts + policy_td))
                    )

                    (
                        df.write.format("iceberg")
                        .mode("overwrite")
                        .option("path",
                                f"{cfg.warehouse}/{cfg.env}/{db_name}/{ice_tbl}")
                        .saveAsTable(full)
                    )

                    n_rows = df.count()
                    written[full] = (n_rows, len(df.columns))
                    print(f"✓ {schema}.{tbl} → {full} ({n_rows} rows)")

    spark.stop()

    # ── summary ──────────────────────────────────────────────
    if written:
        print("\nSummary\n-------")
        total_rows = sum(r for r, _ in written.values())
        for table, (rows, cols) in written.items():
            print(f"{table:<50} {rows:>10} rows   {cols:>3} cols")
        print(f"\nTotal tables: {len(written)}   Total rows: {total_rows}")


if __name__ == "__main__":
    main()
