"""Database helper: create SQLAlchemy engine with simple retry/backoff."""
from __future__ import annotations

import os
import time
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine(database_url: Optional[str] = None, retries: int = 5, backoff: float = 1.0) -> Engine:
    database_url = database_url or os.getenv("DATABASE_URL") or "postgres://etl_user:etl_pass@localhost:5432/etl_db"
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(database_url, future=True)
            # try connect
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            return engine
        except Exception as exc:
            last_exc = exc
            time.sleep(backoff * attempt)
    raise last_exc
