import boto3
import json
import os
import pg8000.dbapi
import oracledb
import re
from urllib.parse import urlparse

glue_client = boto3.client("glue")
secrets_manager_client = boto3.client("secretsmanager")

CA_CERT_PATH = '/var/task/certs/ewallet.pem'  # Oracle wallet path

def get_connection_info(glue_connection_name):
    """
    Fetch JDBC connection URL and secret credentials from AWS Glue and Secrets Manager.
    """
    response = glue_client.get_connection(Name=glue_connection_name)
    props = response['Connection']['ConnectionProperties']
    jdbc_url = props['JDBC_CONNECTION_URL']
    secret_id = props['SECRET_ID']

    secret = secrets_manager_client.get_secret_value(SecretId=secret_id)
    credentials = json.loads(secret['SecretString'])

    return jdbc_url, credentials

def connect_postgres(jdbc_url, credentials):
    parsed = urlparse(jdbc_url.replace("jdbc:", ""))
    return pg8000.dbapi.connect(
        host=parsed.hostname,
        port=parsed.port,
        database=parsed.path.strip("/"),
        user=credentials['username'],
        password=credentials['password']
    )

def parse_oracle_url(jdbc_url):
    pattern = r'@//([^:/]+):(\d+)/(.*)'
    match = re.search(pattern, jdbc_url)
    if not match:
        raise ValueError("Invalid Oracle JDBC URL format")
    return match.groups()  # host, port, service_name

def connect_oracle(jdbc_url, credentials):
    host, port, service_name = parse_oracle_url(jdbc_url)
    dsn = (
        f"(DESCRIPTION="
        f"(ADDRESS=(PROTOCOL=tcps)(HOST={host})(PORT={port}))"
        f"(CONNECT_DATA=(SERVICE_NAME={service_name})))"
    )
    return oracledb.connect(
        user=credentials['username'],
        password=credentials['password'],
        dsn=f"{host}:{port}/{service_name}",
        wallet_location="certs/",
        protocol="tcps",
        ssl_server_dn_match="CN=DataOrchestrationAWS_NFT, OU=Devices, OU=Proving G1 PKI Service, O=The Royal Bank of Scotland plc"
    )

def get_db_connection(glue_connection_name):
    jdbc_url, credentials = get_connection_info(glue_connection_name)

    if "postgresql" in jdbc_url:
        print("Detected PostgreSQL database.")
        return connect_postgres(jdbc_url, credentials), "postgres"
    elif "oracle" in jdbc_url:
        print("Detected Oracle database.")
        return connect_oracle(jdbc_url, credentials), "oracle"
    else:
        raise ValueError("Unsupported JDBC URL/database type")

def discover_tables(cursor, db_type, schemas):
    tables = []
    if db_type == "postgres":
        placeholders = ', '.join(['%s'] * len(schemas))
        sql = f"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ({placeholders})"
        cursor.execute(sql, schemas)

    elif db_type == "oracle":
        for schema in schemas:
            cursor.execute(f"""
                SELECT '{schema}', table_name
                FROM all_tables
                WHERE owner = :owner
            """, [schema.upper()])  # Oracle is case-sensitive

    for row in cursor.fetchall():
        tables.append({"schema": row[0], "table": row[1]})
    return tables

def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))
    
    connection_name = event.get("connection")
    include_rules = event.get("include", {})
    exclude_rules = event.get("exclude", {})
    
    if not connection_name or not include_rules:
        raise ValueError("Event must include 'connection' and 'include' keys.")
    
    schemas = include_rules.get("schemas", [])
    if not schemas:
        raise ValueError("Include rules must specify at least one schema.")
    
    conn = None
    try:
        conn, db_type = get_db_connection(connection_name)
        cursor = conn.cursor()
        all_tables = discover_tables(cursor, db_type, schemas)
        cursor.close()
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        if conn:
            conn.close()

    include_patterns = [re.compile(p) for p in include_rules.get("tables", [])]
    exclude_patterns = [re.compile(p) for p in exclude_rules.get("tables", [])]

    discovered = []
    for item in all_tables:
        name = item["table"]
        included = not include_patterns or any(p.match(name) for p in include_patterns)
        excluded = any(p.match(name) for p in exclude_patterns)
        if included and not excluded:
            discovered.append(item)

    return {
        "source_name": event.get("name"),
        "discovered_tables": discovered
    }
