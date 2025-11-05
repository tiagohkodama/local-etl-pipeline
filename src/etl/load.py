"""Load transformed data into Postgres using SQLAlchemy."""
from __future__ import annotations

from typing import Iterable, Dict

from sqlalchemy import text

from .db import get_engine
from .logger import get_logger

logger = get_logger(__name__)


TABLE_SQL = """
CREATE TABLE IF NOT EXISTS etl.results (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  total_amount NUMERIC,
  created_at TIMESTAMP DEFAULT now()
);
"""


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(TABLE_SQL))


def insert_aggregates(engine, rows: Iterable[Dict[str, object]]) -> int:
    """Insert aggregated rows. Returns number of inserted rows."""
    inserted = 0
    with engine.begin() as conn:
        for r in rows:
            conn.execute(
                text("INSERT INTO etl.results (date, total_amount) VALUES (:date, :total_amount)"),
                {"date": r.get("date"), "total_amount": r.get("total_amount")},
            )
            inserted += 1
    logger.info("Inserted %d aggregated rows", inserted)
    return inserted


def load_from_aggregates(database_url: str | None, aggregates) -> int:
    engine = get_engine(database_url)
    ensure_table(engine)
    return insert_aggregates(engine, aggregates)
