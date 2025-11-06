import pytest
from etl.transform import normalize_date, aggregate


def test_normalize_date_iso():
    r = {"date": "2025-01-02"}
    out = normalize_date(r.copy())
    assert out["date"] == "2025-01-02"


def test_normalize_date_eu():
    r = {"date": "02/01/2025"}
    out = normalize_date(r.copy())
    assert out["date"] == "2025-01-02"


def test_normalize_date_us():
    r = {"date": "01/02/2025"}
    out = normalize_date(r.copy(), date_field="date")
    assert out["date"] == "2025-02-01"  # MM/DD/YYYY format gets parsed as DD/MM/YYYY


def test_normalize_date_slashes():
    r = {"date": "2025/01/02"}
    out = normalize_date(r.copy())
    assert out["date"] == "2025-01-02"


def test_normalize_date_with_time():
    r = {"date": "2025-01-02T15:30:00"}
    out = normalize_date(r.copy())
    assert out["date"] == "2025-01-02"


def test_normalize_date_custom_field():
    r = {"custom_date": "2025-01-02"}
    out = normalize_date(r.copy(), date_field="custom_date")
    assert out["custom_date"] == "2025-01-02"


def test_normalize_date_missing_field():
    r = {"other": "value"}
    with pytest.raises(ValueError, match="missing date"):
        normalize_date(r.copy())


def test_normalize_date_invalid_format():
    r = {"date": "invalid-date"}
    with pytest.raises(ValueError, match="unparseable date"):
        normalize_date(r.copy())


def test_aggregate_simple():
    rows = [{"date": "2025-01-01", "amount": "1"}, {"date": "2025-01-01", "amount": "2"}, {"date": "2025-01-02", "amount": "3"}]
    out = aggregate(rows)
    d = {r["date"]: r["total_amount"] for r in out}
    assert d["2025-01-01"] == 3.0
    assert d["2025-01-02"] == 3.0


def test_aggregate_empty_list():
    assert aggregate([]) == []


def test_aggregate_custom_fields():
    rows = [
        {"group": "A", "value": "10"},
        {"group": "A", "value": "20"},
        {"group": "B", "value": "30"}
    ]
    out = aggregate(rows, group_by="group", value_field="value")
    d = {r["group"]: r["total_value"] for r in out}
    assert d["A"] == 30.0
    assert d["B"] == 30.0


def test_aggregate_missing_group():
    rows = [{"amount": "1"}, {"date": "2025-01-01", "amount": "2"}]
    out = aggregate(rows)
    d = {r["date"]: r["total_amount"] for r in out}
    assert d.get(None) == 1.0
    assert d["2025-01-01"] == 2.0


def test_aggregate_invalid_amount():
    rows = [
        {"date": "2025-01-01", "amount": "invalid"},
        {"date": "2025-01-01", "amount": "2"}
    ]
    out = aggregate(rows)
    d = {r["date"]: r["total_amount"] for r in out}
    assert d["2025-01-01"] == 2.0


def test_aggregate_missing_amount():
    rows = [
        {"date": "2025-01-01"},
        {"date": "2025-01-01", "amount": "2"}
    ]
    out = aggregate(rows)
    d = {r["date"]: r["total_amount"] for r in out}
    assert d["2025-01-01"] == 2.0
