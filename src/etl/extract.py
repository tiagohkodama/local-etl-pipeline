"""CSV extraction and validation."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List

from .logger import get_logger

logger = get_logger(__name__)


def read_csv_folder(folder: str | Path) -> Iterable[Dict[str, str]]:
    """Yield rows from CSV files in folder.

    Malformed rows are logged and skipped.
    """
    folder = Path(folder)
    files = list(folder.glob("*.csv"))
    if not files:
        logger.info("No CSV files found in %s", folder)
        return

    for f in files:
        with f.open("r", encoding="utf8") as fh:
            reader = csv.DictReader(fh)
            for i, row in enumerate(reader, start=1):
                # basic validation: date and amount present
                if not row.get("date") or not row.get("amount"):
                    logger.warning("Skipping bad row %s in %s: missing date or amount", i, f.name)
                    continue
                yield {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
