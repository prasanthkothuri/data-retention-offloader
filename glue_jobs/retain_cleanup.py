from pyspark.sql import SparkSession
from offloadlib.config import load
from datetime import datetime, timedelta
import argparse, re

def dur(txt):
    num, unit = int(txt[:-1]), txt[-1]
    return {'d': timedelta(days=num), 'm': timedelta(days=30*num), 'y': timedelta(days=365*num)}[unit]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', required=True)
    ap.add_argument('--env', required=True)
    args = ap.parse_args()
    cfg = load(args.config)
    spark = (SparkSession.builder
             .appName('retain-cleanup')
             .config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
             .config('spark.sql.catalog.glue_catalog.catalog-impl','org.apache.iceberg.aws.glue.GlueCatalog')
             .config('spark.sql.catalog.glue_catalog.warehouse', f's3://lakehouse/{args.env}')
             .getOrCreate())
    now = datetime.utcnow()
    for src in cfg.sources:
        cutoff = (now - dur(cfg.retention[src.retention_policy])).strftime('%Y-%m-%dT%H:%M:%S')
        for schema in src.include['schemas']:
            db_name = f'{args.env}_{schema}'
            for pat in src.include['tables']:
                tbl = re.sub(r'[^a-z0-9_]', '_', pat.lower())
                full = f'glue_catalog.{db_name}.{tbl}'
                spark.sql(f"CALL glue_catalog.system.expire_snapshots(table => '{full}', older_than => TIMESTAMP '{cutoff}')")
                spark.sql(f"CALL glue_catalog.system.remove_orphan_files(table => '{full}')")
    spark.stop()

if __name__ == '__main__':
    main()
