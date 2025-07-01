from __future__ import annotations
import os
import yaml
from dataclasses import dataclass
from typing import Dict, List


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


def load(path: str = "conf/offload.yaml") -> Settings:
    with open(path) as fh:
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
