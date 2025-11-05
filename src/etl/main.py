"""CLI entrypoint for ETL run."""
from __future__ import annotations

import argparse
import os
from typing import List

from .config import load_config
from .extract import read_csv_folder
from .transform import normalize_date, aggregate
from .load import load_from_aggregates
from .logger import get_logger

logger = get_logger(__name__)


def run(input_folder: str, dry_run: bool = False, database_url: str | None = None) -> int:
    """Run the ETL: extract, transform, aggregate, load.

    Returns number of inserted rows (0 if dry-run).
    """
    logger.info("Starting ETL on %s (dry_run=%s)", input_folder, dry_run)
    rows = []
    for r in read_csv_folder(input_folder):
        try:
            r2 = normalize_date(r)
            rows.append(r2)
        except Exception as exc:
            logger.warning("Skipping row due to transform error: %s", exc)

    aggregates = aggregate(rows, group_by="date", value_field="amount")
    logger.info("Aggregated into %d groups", len(aggregates))

    if dry_run:
        logger.info("Dry run - would insert: %s", aggregates)
        return 0

    cfg = load_config()
    database_url = database_url or os.getenv("DATABASE_URL") or cfg.database_url
    inserted = load_from_aggregates(database_url, [{"date": a["date"], "total_amount": a[f"total_amount"]} for a in aggregates])
    logger.info("ETL finished, inserted=%d", inserted)
    return inserted


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="etl")
    parser.add_argument("--input", "-i", default=os.getenv("INPUT_FOLDER", "/data"))
    parser.add_argument("--dry-run", action="store_true", dest="dry_run")
    args = parser.parse_args(argv)
    run(args.input, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
