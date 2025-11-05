"""Transformation logic: normalize dates and aggregate amounts."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List

from .logger import get_logger

logger = get_logger(__name__)


def normalize_date(row: Dict[str, Any], date_field: str = "date") -> Dict[str, Any]:
    """Normalize various date formats to ISO 8601 (YYYY-MM-DD).

    Raises ValueError if the date cannot be parsed.
    """
    raw = row.get(date_field)
    if not raw:
        raise ValueError("missing date")

    # common formats
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            row[date_field] = dt.date().isoformat()
            return row
        except Exception:
            continue

    # try fromisoformat
    try:
        row[date_field] = datetime.fromisoformat(raw).date().isoformat()
        return row
    except Exception as exc:
        raise ValueError(f"unparseable date: {raw}") from exc


def aggregate(rows: Iterable[Dict[str, Any]], group_by: str = "date", value_field: str = "amount") -> List[Dict[str, Any]]:
    """Aggregate numeric `value_field` grouped by `group_by`.

    Returns list of dicts with keys group_by and total_{value_field}.
    """
    result: dict[str, float] = {}
    for r in rows:
        key = r.get(group_by)
        try:
            val = float(r.get(value_field, 0))
        except Exception:
            logger.warning("Non-numeric value for %s: %s", value_field, r.get(value_field))
            continue
        result.setdefault(key, 0.0)
        result[key] += val

    return [{group_by: k, f"total_{value_field}": v} for k, v in result.items()]
