"""Configuration loader for the ETL.

Reads DATABASE_URL from the environment or `config.yml`.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import yaml


@dataclass
class Config:
    database_url: str


def load_config(path: str = "config.yml") -> Config:
    # env has precedence
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return Config(database_url=db_url)

    if os.path.exists(path):
        with open(path, "r", encoding="utf8") as fh:
            raw = yaml.safe_load(fh) or {}
            db_url = raw.get("database", {}).get("url")
            if db_url:
                return Config(database_url=db_url)

    # fallback
    return Config(database_url="postgres://etl_user:etl_pass@localhost:5432/etl_db")
