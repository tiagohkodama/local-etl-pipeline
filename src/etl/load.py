"""Load transformed data into Postgres using SQLAlchemy."""
from __future__ import annotations

from typing import Iterable, Dict

from sqlalchemy import text

from .db import get_engine
from .logger import get_logger

logger = get_logger(__name__)


TABLE_SQL = """
CREATE TABLE IF NOT EXISTS results (
  id BIGSERIAL PRIMARY KEY,
  date DATE NOT NULL,
  total_amount NUMERIC,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
"""



def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(TABLE_SQL))


import datetime
from numbers import Number

def insert_aggregates(engine, rows):
    inserted = 0
    with engine.begin() as conn:
        for r in rows:
            d = r.get("date")
            amt = r.get("total_amount")

            # --- HARD SANITY CHECK ---
            if not isinstance(d, (datetime.date, datetime.datetime)):
                logger.warning("Skipping row - invalid date: %r", d)
                continue
            if not isinstance(amt, Number):
                logger.warning("Skipping row - invalid amount: %r", amt)
                continue
            # -------------------------

            conn.execute(
                text("INSERT INTO results (date, total_amount) VALUES (:date, :total_amount)"),
                {"date": d, "total_amount": amt},
            )
            inserted += 1
    logger.info("Inserted %d aggregated rows", inserted)
    return inserted



def load_from_aggregates(database_url: str | None, aggregates) -> int:
    engine = get_engine(database_url)
    ensure_table(engine)
    return insert_aggregates(engine, aggregates)
