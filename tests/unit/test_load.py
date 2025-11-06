"""Unit tests for load module."""
import pytest
from sqlalchemy import create_engine, text
from unittest.mock import Mock, patch


import os
from etl.load import ensure_table, insert_aggregates, load_from_aggregates
@pytest.fixture
def test_db(tmp_path):
    """Create a test SQLite database and clean up after."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    yield engine
    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def mock_engine():
    """Create a mock engine."""
    engine = Mock()
    conn = Mock()
    engine.begin = Mock(return_value=conn)
    engine.connect = Mock(return_value=conn)
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=None)
    conn.execute = Mock(return_value=None)
    return engine


def test_ensure_table(mock_engine):
    """Test table creation."""
    ensure_table(mock_engine)
    
    # Verify execute was called with CREATE TABLE
    mock_engine.begin().__enter__().execute.assert_called_once()
    call_args = mock_engine.begin().__enter__().execute.call_args[0][0]
    assert "CREATE TABLE" in str(call_args)
    assert "results" in str(call_args)


def test_insert_aggregates(mock_engine):
    """Test inserting aggregated data."""
    rows = [
        {"date": "2025-01-01", "total_amount": "100.50"},
        {"date": "2025-01-02", "total_amount": "200.75"}
    ]
    
    count = insert_aggregates(mock_engine, rows)
    assert count == 2
    
    # Verify execute was called twice with INSERT
    assert mock_engine.begin().__enter__().execute.call_count == 2
    for call in mock_engine.begin().__enter__().execute.call_args_list:
        assert "INSERT INTO" in str(call[0][0])


def test_insert_aggregates_empty(mock_engine):
    """Test inserting empty data."""
    count = insert_aggregates(mock_engine, [])
    assert count == 0
    
    # Verify execute was not called
    mock_engine.begin().__enter__().execute.assert_not_called()


def test_insert_aggregates_null_values(mock_engine):
    """Test inserting rows with null values."""
    rows = [
        {"date": "2025-01-01", "total_amount": None},
        {"date": "2025-01-02", "total_amount": "100.50"}
    ]
    
    count = insert_aggregates(mock_engine, rows)
    assert count == 2
    
    # Verify execute was called twice
    assert mock_engine.begin().__enter__().execute.call_count == 2





def test_insert_aggregates(mock_engine):
    """Test inserting aggregated data (mocked)."""
    rows = [
        {"date": "2025-01-01", "total_amount": "100.50"},
        {"date": "2025-01-02", "total_amount": "200.75"}
    ]
    count = insert_aggregates(mock_engine, rows)
    assert count == 2
    # Verify execute was called twice with INSERT
    assert mock_engine.begin().__enter__().execute.call_count == 2
    for call in mock_engine.begin().__enter__().execute.call_args_list:
        assert "INSERT INTO" in str(call[0][0])


def test_insert_aggregates_empty(mock_engine):
    """Test inserting empty data (mocked)."""
    count = insert_aggregates(mock_engine, [])
    assert count == 0
    # Verify execute was not called
    mock_engine.begin().__enter__().execute.assert_not_called()


def test_insert_aggregates_null_values(mock_engine):
    """Test inserting rows with null values (mocked)."""
    rows = [
        {"date": "2025-01-01", "total_amount": None},
        {"date": "2025-01-02", "total_amount": "100.50"}
    ]
    count = insert_aggregates(mock_engine, rows)
    assert count == 2
    # Verify execute was called twice
    assert mock_engine.begin().__enter__().execute.call_count == 2


def test_load_from_aggregates_with_mock_engine(monkeypatch, test_db):
    """Test the full load process with a mock engine."""
    rows = [
        {"date": "2025-01-01", "total_amount": "100.50"},
        {"date": "2025-01-02", "total_amount": "200.75"}
    ]
    
    def mock_get_engine(url):
        return test_db
        
    monkeypatch.setattr("etl.db.get_engine", mock_get_engine)
    
    count = load_from_aggregates(f"sqlite:///{test_db.url.database}", rows)
    assert count == 2
    
    # Verify data was loaded
    with test_db.begin() as conn:
        result = conn.execute(text(
            "SELECT count(*) FROM results"
        )).fetchone()
        assert result[0] == 2

