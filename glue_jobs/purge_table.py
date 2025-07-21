import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

# ----------------------------------
# Read job parameters
# ----------------------------------
args = getResolvedOptions(sys.argv, [
    'database',
    'table',
    'retention_expiry_column',
    'legal_hold_column',
    'iceberg_warehouse'
])

database = args['database']
table = args['table']
retention_col = args['retention_expiry_column']
legal_hold_col = args['legal_hold_column']
iceberg_warehouse = args['iceberg_warehouse']

# ----------------------------------
# Initialize Spark & Glue
# ----------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder \
    .appName("Iceberg Retention Policy Delete Job") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", iceberg_warehouse) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

job = Job(glueContext)

# ----------------------------------
# Setup Table and Columns
# ----------------------------------
catalog = "glue_catalog"
full_table_name = f"{catalog}.{database}.{table}"
retention_col_escaped = f"`{retention_col}`"
legal_hold_col_escaped = f"`{legal_hold_col}`"

# Get today's date
today = datetime.today().strftime('%Y-%m-%d')

print(f"[INFO] Starting retention delete job for table: {full_table_name}")
print(f"[INFO] Current Date: {today}")

# ----------------------------------
# Step 1: Check Column Existence
# ----------------------------------
print(f"[INFO] Checking schema for columns: {retention_col}, {legal_hold_col}")
schema_df = spark.sql(f"DESCRIBE TABLE {full_table_name}")
columns = [row['col_name'] for row in schema_df.collect()]

if retention_col not in columns:
    raise Exception(f"[ERROR] Retention column '{retention_col}' not found in table schema.")
if legal_hold_col not in columns:
    raise Exception(f"[ERROR] Legal hold column '{legal_hold_col}' not found in table schema.")

print("[INFO] Required columns found.")

# ----------------------------------
# Step 2: Build WHERE clause
# ----------------------------------
where_clause = f"{legal_hold_col_escaped} = false AND {retention_col_escaped} <= DATE('{today}')"

# ----------------------------------
# Step 3: Check matching records
# ----------------------------------
check_query = f"SELECT COUNT(*) AS match_count FROM {full_table_name} WHERE {where_clause}"
print(f"[INFO] Executing match check query: {check_query}")

match_count = spark.sql(check_query).collect()[0]["match_count"]
print(f"[INFO] Matching records for deletion: {match_count}")

if match_count == 0:
    print("[INFO] No records to delete — job complete.")
else:
    print("[INFO] Proceeding to delete records...")

    # ----------------------------------
    # Step 4: DELETE operation
    # ----------------------------------
    delete_query = f"DELETE FROM {full_table_name} WHERE {where_clause}"
    print(f"[INFO] Executing DELETE query: {delete_query}")
    spark.sql(delete_query)
    print("[INFO] Delete operation completed.")

    # ----------------------------------
    # Step 5: Post-deletion verification
    # ----------------------------------
    recheck_count = spark.sql(check_query).collect()[0]["match_count"]
    if recheck_count == 0:
        print("[INFO] Verification successful — records have been deleted.")
    else:
        print(f"[WARNING] Verification failed — {recheck_count} records still present.")

job.commit()
print("[INFO] Glue job completed.")