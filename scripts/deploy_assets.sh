#!/usr/bin/env bash
#
# Build offloadlib wheel and upload all Glue assets.
# Optional: pass --with-job to auto-create or update Glue job.
#
set -euo pipefail

## ---- CONFIG --------------------------------------------------------
BUCKET="bitbio.temp"
PREFIX_SCRIPTS="scripts"
PREFIX_LIBS="libs"
PREFIX_CONFIGS="configs"
GLUE_JOB_NAME="bulk-offload"
GLUE_ROLE_ARN="arn:aws:iam::017309998751:role/service-role/AWSGlueServiceRole-datalake"
GLUE_CONN_NAME="nucleus_dev"
DB_USER="nucleus_dev"
DB_PASS="k5XW66TL05d6"
JDBC_URL="jdbc:postgresql://nucleus-database-instance-1.cd55af1gjius.eu-west-2.rds.amazonaws.com:5432/nucleus_dev"
AWS_REGION="eu-west-2"
## --------------------------------------------------------------------

WITH_JOB=false
if [[ ${1:-} == "--with-job" ]]; then
  WITH_JOB=true
fi

echo "• Cleaning old builds"
rm -rf build/ dist/ offloadlib/__pycache__

echo "• Building wheel"
python -m build --wheel >/dev/null
WHEEL_PATH=$(ls dist/offloadlib-*.whl | head -n1)
WHEEL_FILE=$(basename "$WHEEL_PATH")

echo "• Uploading wheel → s3://$BUCKET/$PREFIX_LIBS/$WHEEL_FILE"
aws s3 cp "$WHEEL_PATH" "s3://$BUCKET/$PREFIX_LIBS/$WHEEL_FILE" --region "$AWS_REGION"

echo "• Uploading driver script(s)"
aws s3 cp glue_jobs/bulk_offload.py "s3://$BUCKET/$PREFIX_SCRIPTS/bulk_offload.py" --region "$AWS_REGION"
aws s3 cp glue_jobs/retain_cleanup.py "s3://$BUCKET/$PREFIX_SCRIPTS/retain_cleanup.py" --region "$AWS_REGION"

echo "• Uploading configuration"
aws s3 cp conf/offload.yaml "s3://$BUCKET/$PREFIX_CONFIGS/offload.yaml" --region "$AWS_REGION"

# ── Create / update Glue connection ───────────────────────────────
echo "• Syncing Glue connection $GLUE_CONN_NAME"
CONN_INPUT=$(cat <<JSON
{
  "Name": "$GLUE_CONN_NAME",
  "ConnectionType": "JDBC",
  "ConnectionProperties": {
    "USERNAME": "$DB_USER",
    "PASSWORD": "$DB_PASS",
    "JDBC_CONNECTION_URL": "$JDBC_URL"
  }
}
JSON
)

aws glue get-connection --name "$GLUE_CONN_NAME" --region "$AWS_REGION" >/dev/null 2>&1 \
  && aws glue update-connection --name "$GLUE_CONN_NAME" --connection-input "$CONN_INPUT" --region "$AWS_REGION" \
  || aws glue create-connection --connection-input "$CONN_INPUT" --region "$AWS_REGION"

# ────────────────────────────────────────────────────────────────
# Glue job create/update logic (fixed command formatting)
# ────────────────────────────────────────────────────────────────
if $WITH_JOB; then
  echo "• Syncing Glue job: $GLUE_JOB_NAME"

  SCRIPT_URI="s3://$BUCKET/$PREFIX_SCRIPTS/bulk_offload.py"
  CONFIG_URI="s3://$BUCKET/$PREFIX_CONFIGS/offload.yaml"
  WHEEL_URI="s3://$BUCKET/$PREFIX_LIBS/$WHEEL_FILE"

  # Create command JSON string (properly formatted)
  COMMAND_JSON=$(cat <<JSON
{
  "Name": "glueetl",
  "ScriptLocation": "$SCRIPT_URI",
  "PythonVersion": "3"
}
JSON
)

  # Create default arguments JSON
  DEFAULT_ARGS=$(cat <<JSON
{
  "--mode": "glue",
  "--env": "prod",
  "--config": "$CONFIG_URI",
  "--extra-py-files": "$WHEEL_URI",
  "--TempDir": "s3://$BUCKET/temp/",
  "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
  "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
  "--conf": "spark.hadoop.fs.s3a.region=$AWS_REGION",
  "--conf": "spark.sql.catalog.glue_catalog.glue.region=$AWS_REGION",
  "--datalake-formats": "iceberg"
}
JSON
)

  set +e
  aws glue get-job --job-name "$GLUE_JOB_NAME" --region "$AWS_REGION" >/dev/null 2>&1
  EXISTS=$?
  set -e

  if [[ $EXISTS -eq 0 ]]; then
    echo "  ↪ Updating existing job"
    aws glue update-job --job-name "$GLUE_JOB_NAME" --region "$AWS_REGION" --job-update "{
      \"Role\": \"$GLUE_ROLE_ARN\",
      \"Command\": $COMMAND_JSON,
      \"GlueVersion\": \"4.0\",
      \"DefaultArguments\": $DEFAULT_ARGS,
      \"NumberOfWorkers\": 10,
      \"WorkerType\": \"G.1X\"
    }"
  else
    echo "  ↪ Creating new job"
    aws glue create-job \
      --name "$GLUE_JOB_NAME" \
      --role "$GLUE_ROLE_ARN" \
      --region "$AWS_REGION" \
      --command "$COMMAND_JSON" \
      --glue-version "4.0" \
      --default-arguments "$DEFAULT_ARGS" \
      --number-of-workers 10 \
      --worker-type "G.1X"
  fi

  echo "✓ Glue job synced."
fi

echo "✓ All assets deployed."
