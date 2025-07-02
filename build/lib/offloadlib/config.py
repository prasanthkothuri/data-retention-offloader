from __future__ import annotations
import os
import yaml
from dataclasses import dataclass
from typing import Dict, List
from urllib.parse import urlparse
from dataclasses import dataclass
import boto3
import io


@dataclass
class Conn:
    jdbc_url: str
    driver: str
    user_env: str
    password_env: str

    def creds(self):
        return os.getenv(self.user_env), os.getenv(self.password_env)


@dataclass
class SourceSpec:
    name: str
    connection: str
    include: Dict[str, List[str]]
    exclude: Dict[str, List[str]] | None
    retention_policy: str


@dataclass
class Settings:
    env: str
    warehouse: str
    connections: Dict[str, Conn]
    sources: List[SourceSpec]
    retention_policies: Dict[str, str]


def _open_any(path: str):
    """Return a file-like object for local or s3:// URI."""
    if path.startswith("s3://"):
        uri = urlparse(path)
        bucket, key = uri.netloc, uri.path.lstrip("/")
        s3 = boto3.client("s3")
        data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return io.StringIO(data.decode())
    return open(path, "r", encoding="utf-8")


def get_glue_creds(conn_name: str):
    glue = boto3.client("glue")
    c = glue.get_connection(Name=conn_name)["Connection"]
    p = c["ConnectionProperties"]
    return p["USERNAME"], p["PASSWORD"], p["JDBC_CONNECTION_URL"]


def load(path: str = "conf/offload.yaml") -> Settings:
    with _open_any(path) as fh:
        raw = yaml.safe_load(fh)

    raw["env"] = os.getenv("GLUE_ENV", raw.get("env", "dev")).lower()

    conns = {k: Conn(**v) for k, v in raw["connections"].items()}
    sources = [SourceSpec(**s) for s in raw["sources"]]

    return Settings(
        env=raw["env"],
        warehouse=raw["warehouse"],
        connections=conns,
        sources=sources,
        retention_policies=raw["retention_policies"],
    )
