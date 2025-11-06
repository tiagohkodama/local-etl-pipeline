"""Unit tests for extract module."""
import os
import tempfile
from pathlib import Path

import pytest

from etl.extract import read_csv_folder


@pytest.fixture
def temp_dir():
    """Create a temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def create_csv_file(folder: str | Path, name: str, content: str) -> Path:
    """Helper to create a CSV file with given content."""
    path = Path(folder) / name
    path.write_text(content, encoding="utf8")
    return path


def test_read_csv_empty_folder(temp_dir):
    """Test reading from an empty folder."""
    rows = list(read_csv_folder(temp_dir))
    assert rows == []


def test_read_csv_single_file(temp_dir):
    """Test reading from a single valid CSV file."""
    content = "date,amount\n2025-01-01,100\n2025-01-02,200"
    create_csv_file(temp_dir, "data.csv", content)
    
    rows = list(read_csv_folder(temp_dir))
    assert len(rows) == 2
    assert rows[0] == {"date": "2025-01-01", "amount": "100"}
    assert rows[1] == {"date": "2025-01-02", "amount": "200"}


def test_read_csv_multiple_files(temp_dir):
    """Test reading from multiple CSV files."""
    create_csv_file(temp_dir, "data1.csv", "date,amount\n2025-01-01,100")
    create_csv_file(temp_dir, "data2.csv", "date,amount\n2025-01-02,200")
    
    rows = list(read_csv_folder(temp_dir))
    assert len(rows) == 2
    dates = {r["date"] for r in rows}
    assert dates == {"2025-01-01", "2025-01-02"}


def test_read_csv_skip_invalid_rows(temp_dir):
    """Test that invalid rows are skipped."""
    content = "date,amount\n2025-01-01,100\n,200\n2025-01-02,\n2025-01-03,300"
    create_csv_file(temp_dir, "data.csv", content)
    
    rows = list(read_csv_folder(temp_dir))
    assert len(rows) == 2
    dates = {r["date"] for r in rows}
    assert dates == {"2025-01-01", "2025-01-03"}


def test_read_csv_strips_whitespace(temp_dir):
    """Test that whitespace is stripped from string values."""
    content = "date,amount\n 2025-01-01 , 100 \n"
    create_csv_file(temp_dir, "data.csv", content)
    
    rows = list(read_csv_folder(temp_dir))
    assert len(rows) == 1
    assert rows[0] == {"date": "2025-01-01", "amount": "100"}


def test_read_csv_ignores_non_csv_files(temp_dir):
    """Test that non-CSV files are ignored."""
    create_csv_file(temp_dir, "data.csv", "date,amount\n2025-01-01,100")
    create_csv_file(temp_dir, "data.txt", "date,amount\n2025-01-02,200")
    
    rows = list(read_csv_folder(temp_dir))
    assert len(rows) == 1
    assert rows[0] == {"date": "2025-01-01", "amount": "100"}


def test_read_csv_with_extra_columns(temp_dir):
    """Test reading CSV with additional columns."""
    content = "date,amount,category\n2025-01-01,100,food\n"
    create_csv_file(temp_dir, "data.csv", content)
    
    rows = list(read_csv_folder(temp_dir))
    assert len(rows) == 1
    assert rows[0] == {"date": "2025-01-01", "amount": "100", "category": "food"}


def test_read_csv_with_empty_strings(temp_dir):
    """Test that empty strings are preserved for non-required fields."""
    content = "date,amount,category\n2025-01-01,100,\n"
    create_csv_file(temp_dir, "data.csv", content)
    
    rows = list(read_csv_folder(temp_dir))
    assert len(rows) == 1
    assert rows[0] == {"date": "2025-01-01", "amount": "100", "category": ""}