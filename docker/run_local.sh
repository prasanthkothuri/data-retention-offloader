#!/usr/bin/env bash
set -euo pipefail

# ── credentials ─────────────────────────────────────────────
export PGUSER="nucleus_dev"
export PGPASSWORD="k5XW66TL05d6"
export AWS_ACCESS_KEY_ID="minio"
export AWS_SECRET_ACCESS_KEY="minio123"

# ── 1. start / keep-alive the stack ─────────────────────────
docker compose -f docker/docker-compose.yml up -d
sleep 5   # give Spark & MinIO a moment

# ── 2. install wheel(s) inside the Spark container ──────────
docker compose -f docker/docker-compose.yml exec spark \
  pip install --no-cache-dir -r /workspace/requirements.txt

# ── 2. ensure 'lakehouse' bucket exists in MinIO ────────────
docker compose -f docker/docker-compose.yml exec minio mc alias set local http://minio:9000 minio minio123 >/dev/null 2>&1 || true
docker compose -f docker/docker-compose.yml exec minio mc ls --insecure local/lakehouse >/dev/null 2>&1 || \
docker compose -f docker/docker-compose.yml exec minio mc mb --insecure local/lakehouse

# ── 3. run the Spark job inside the Spark container ─────────
docker compose -f docker/docker-compose.yml exec \
  -e PGUSER \
  -e PGPASSWORD \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  spark spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.postgresql:postgresql:42.7.3 \
    glue_jobs/bulk_offload.py \
    --config conf/offload.yaml \
    --env dev \
    --mode local
