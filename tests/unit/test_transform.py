from etl.transform import normalize_date, aggregate


def test_normalize_date_iso():
    r = {"date": "2025-01-02"}
    out = normalize_date(r.copy())
    assert out["date"] == "2025-01-02"


def test_normalize_date_eu():
    r = {"date": "02/01/2025"}
    out = normalize_date(r.copy())
    assert out["date"] == "2025-01-02"


def test_aggregate_simple():
    rows = [{"date": "2025-01-01", "amount": "1"}, {"date": "2025-01-01", "amount": "2"}, {"date": "2025-01-02", "amount": "3"}]
    out = aggregate(rows)
    d = {r["date"]: r["total_amount"] for r in out}
    assert d["2025-01-01"] == 3.0
    assert d["2025-01-02"] == 3.0
