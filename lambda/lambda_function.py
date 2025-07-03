import json
import boto3
import yaml
import re
import logging
from sqlalchemy import create_engine, inspect
from urllib.parse import quote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def load_yaml_from_s3(bucket, key):
    s3 = boto3.client('s3')
    logger.info(f"📥 Loading YAML from s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    config = yaml.safe_load(response['Body'])
    logger.info("✅ YAML loaded successfully")
    return config

def get_glue_connection_details(connection_name):
    glue = boto3.client('glue')
    logger.info(f"🔌 Fetching Glue connection details for '{connection_name}'")
    resp = glue.get_connection(Name=connection_name)
    props = resp['Connection']['ConnectionProperties']
    jdbc = props["JDBC_CONNECTION_URL"]
    logger.info(f"🌐 JDBC URL: {jdbc}")

    if jdbc.startswith("jdbc:sqlserver://"):
        pattern = r"jdbc:sqlserver://(?P<host>[^:;]+):(?P<port>\d+);database=(?P<db>[^;]+)"
        m = re.match(pattern, jdbc)
        if not m:
            raise ValueError(f"❌ Unable to parse JDBC URL: {jdbc}")
        host, port, database = m.group("host"), m.group("port"), m.group("db")
    else:
        pattern = (
            r"jdbc:(?P<dialect>[^:]+)://"
            r"(?P<host>[^:/]+):(?P<port>\d+)[/:](?P<db1>[A-Za-z0-9_]+)"
            r"(?:;database=(?P<db2>[A-Za-z0-9_]+))?"
        )
        m = re.match(pattern, jdbc)
        if not m:
            raise ValueError(f"❌ Unable to parse JDBC URL: {jdbc}")
        host, port = m.group("host"), m.group("port")
        database = m.group("db2") or m.group("db1")

    logger.info(f"🔍 Parsed Host: {host}, Port: {port}, DB: {database}")

    return {
        "username": props.get("USERNAME"),
        "password": props.get("PASSWORD"),
        "host": host,
        "port": int(port),
        "database": database,
        "url": jdbc
    }

def build_sqlalchemy_engine(c):
    jdbc = c['url']
    if jdbc.startswith("jdbc:mysql://"):
        dialect = "mysql+pymysql"
    elif jdbc.startswith("jdbc:postgresql://"):
        dialect = "postgresql+psycopg2"
    elif jdbc.startswith("jdbc:sqlserver://"):
        dialect = "mssql+pymssql"
    elif jdbc.startswith("jdbc:oracle://"):
        dialect = "oracle+oracledb"
    else:
        raise Exception(f"❌ Unsupported JDBC URL: {jdbc}")

    logger.info(f"⚙️ Creating SQLAlchemy engine: {dialect}")
    url = (
        f"{dialect}://{c['username']}:{quote_plus(c['password'])}"
        f"@{c['host']}:{c['port']}/{c['database']}"
    )
    return create_engine(url)

def discover_tables(conn_name, schemas, regex):
    c = get_glue_connection_details(conn_name)
    eng = build_sqlalchemy_engine(c)
    insp = inspect(eng)
    out = []

    logger.info("🔍 Inspecting schemas...")
    for sch in insp.get_schema_names():
        if schemas and sch not in schemas:
            logger.info(f"⏭️ Skipping schema '{sch}'")
            continue
        logger.info(f"🔎 Schema: {sch}")

        for t in insp.get_table_names(schema=sch):
            logger.info(f"  📄 Found table: {t}")
            if re.fullmatch(regex, t):
                full = f"{sch}.{t}"
                logger.info(f"✅ Matched: {full}")
                out.append({
                    "datastore_type": eng.name,
                    "database_name": c["database"],
                    "table_name": full,
                    "connection_name": conn_name,
                    "iceberg_db": c["database"],
                    "iceberg_table": f"{t}_iceberg"
                })
            else:
                logger.info(f"❌ No match: '{t}'")

    return out

def trigger_glue_job(job, args):
    glue = boto3.client('glue')
    logger.info(f"🚀 Triggering {job} with args: {args}")
    return glue.start_job_run(JobName=job, Arguments=args)

def lambda_handler(event, context):
    try:
        rec = event['Records'][0]['s3']
        cfg = load_yaml_from_s3(rec['bucket']['name'], rec['object']['key'])
        wh = cfg.get('iceberg_warehouse')
        conmap = cfg.get('connections', {})

        job_args_list = []     
        
        for src in cfg.get('sources', []):
            conn_key = src['connection']
            glue_conn = conmap.get(conn_key)
            if not glue_conn:
                logger.warning(f"❗ Missing mapping for '{conn_key}'")
                continue

            logger.info(f"🔁 Processing '{conn_key}'")
            tables = discover_tables(
                glue_conn,
                src.get('include', {}).get('schemas', []),
                src.get('include', {}).get('tables', ["*"])[0]
            )

            for t in tables:
                args = {
                    "--datastore_type": t["datastore_type"],
                    "--database_name": t["database_name"],
                    "--table_name": t["table_name"],
                    "--connection_name": t["connection_name"],
                    "--iceberg_db": t["iceberg_db"],
                    "--iceberg_table": t["iceberg_table"],
                    "--iceberg_warehouse": wh
                }
                logger.info(json.dumps(args))
                job_args_list.append(args)
                # trigger_glue_job("archival_job", args)  # ⛔️ disabled for local testing

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Discovery complete",
                "job_args": job_args_list  # ✅ Include in response
            })
        }
    except Exception as e:
        logger.error("❌ Fatal error", exc_info=True)
        return {"statusCode": 500, "body": json.dumps(str(e))}
