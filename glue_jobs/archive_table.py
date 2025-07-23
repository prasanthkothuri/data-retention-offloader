# archive_table.py
import sys
import re
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def create_glue_database_if_not_exists(db_name: str, region: str = "eu-west-1"):
    glue = boto3.client("glue", region_name=region)
    try:
        glue.get_database(Name=db_name)
        print(f"Glue database '{db_name}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"Glue database '{db_name}' not found. Creating it …")
            try:
                glue.create_database(DatabaseInput={"Name": db_name})
                print(f"Glue database '{db_name}' created.")
            except ClientError as create_error:
                if create_error.response["Error"]["Code"] == "AlreadyExistsException":
                    print(f"Glue database '{db_name}' was created by another job.")
                else:
                    raise
        else:
            raise

def _parse_retention_duration(text: str) -> timedelta:
    m = re.match(r"^(\d+)([dmy])$", text, re.I)
    if not m:
        raise ValueError(f"Unsupported duration literal: {text!r}")
    num, unit = int(m[1]), m[2].lower()
    return timedelta(days=num * {"d": 1, "m": 30, "y": 365}[unit])

# --------------------------------------------------------------------------- #
# Arg parsing
# --------------------------------------------------------------------------- #
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_schema",
        "source_table",
        "glue_connection_name",
        "target_s3_path",
        "target_glue_db",
        "target_glue_table",
        "retention_policy_value",
        "legal_hold",
    ],
)

print(f"Starting archival for table: {args['source_schema']}.{args['source_table']}")

# --------------------------------------------------------------------------- #
# Initialize Spark/Glue context with Iceberg support
# --------------------------------------------------------------------------- #
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

parsed = urlparse(args["target_s3_path"])
warehouse_root = f"s3://{parsed.netloc}/iceberg/"

# These should already be present in --default-arguments, but also kept here for clarity
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_root)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# --------------------------------------------------------------------------- #
# Get JDBC connection info from Glue to extract connection type
# --------------------------------------------------------------------------- #
glue = boto3.client("glue")
conn = glue.get_connection(Name=args["glue_connection_name"])['Connection']
conn_type = conn['ConnectionType']
conn_type = "oracle"
# conn_properties = conn['ConnectionProperties']

# jdbc_url = conn_properties['JDBC_CONNECTION_URL']
# secret_id = conn_properties['SECRET_ID']
# driver_class = conn_properties['JDBC_DRIVER_CLASS_NAME']

# # --- 2. Fetch the Secret from AWS Secrets Manager ---
# secrets_client = boto3.client('secretsmanager')
# get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_id)
# # Secrets Manager stores credentials as a JSON string
# secret = json.loads(get_secret_value_response['SecretString'])
# db_username = secret['username']
# db_password = secret['password']
# server_dn = conn_properties['CUSTOM_JDBC_CERT_STRING']
# server_dn = "CN=DataOrchestrationAWS_NFT, OU=Devices, OU=Proving G1 PKI Service, O=The Royal Bank of Scotland plc"

# --------------------------------------------------------------------------- #
# Read source table using GlueContext.getSource()
# --------------------------------------------------------------------------- #
# --- 3. Read Data Using the Discovered SSL Options ---
# df = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", f"{args['source_schema']}.{args['source_table']}") \
#     .option("user", db_username) \
#     .option("password", db_password) \
#     .option("driver", driver_class) \
#     .option("oracle.net.ssl_server_dn_match", "true") \
#     .option("oracle.net.ssl_server_cert_dn", server_dn) \
#     .option("javax.net.ssl.trustStore", "/opt/amazon/certs/InternalAndExternalAndAMSTrustStore.jks") \
#     .option("javax.net.ssl.trustStoreType", "JKS") \
#     .option("javax.net.ssl.trustStorePassword", "amazon") \
#     .load()

connection_options = {
        "useConnectionProperties": "true",
        "dbtable": 'edi_sup_owner.feed',
        "connectionName": 'onprem_orcl_conn',
    }

# get dataframe
# df = glueContext.getSource(
#     connection_type = "oracle",
#     options = connection_options
# ).getFrame()

df = glueContext.create_dynamic_frame.from_options(
    connection_type = "oracle",
    connection_options = connection_options
).toDF()

print(f"Read {df.count()} rows from {args['source_schema']}.{args['source_table']}")

# --------------------------------------------------------------------------- #
# Add retention columns
# --------------------------------------------------------------------------- #
legal_hold = args['legal_hold'].lower() == 'true'
now = datetime.utcnow()
expires = now + _parse_retention_duration(args['retention_policy_value'])

df = (
    df.withColumn("archived_at", F.lit(now))
      .withColumn("retention_expires_at", F.lit(expires))
      .withColumn("legal_hold", F.lit(legal_hold))
)

# --------------------------------------------------------------------------- #
# Write to Iceberg
# --------------------------------------------------------------------------- #
create_glue_database_if_not_exists(args["target_glue_db"])

iceberg_table = f"`glue_catalog`.`{args['target_glue_db']}`.`{args['target_glue_table']}`"
print(f"Writing to Iceberg table: {iceberg_table}")

# (
#     df.write.format("iceberg")
#       .mode("overwrite")
#       .option("path", args["target_s3_path"])
#       .saveAsTable(iceberg_table)
# )

df.writeTo(f"glue_catalog.{args['target_glue_db']}.{args['target_glue_table'].lower()}").using("iceberg").createOrReplace()

print("Archive complete.")
job.commit()
