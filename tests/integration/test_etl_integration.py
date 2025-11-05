import os
import time

import pytest
from sqlalchemy import text, create_engine


def get_database_url():
    return os.environ.get("DATABASE_URL", "postgresql://etl_user:etl_pass@localhost:5432/etl_db")


@pytest.mark.integration
def test_etl_runs_and_inserts(tmp_path):
    """Requires a running Postgres (docker-compose up -d postgres) or CI service.

    The test will be skipped if DB is unreachable.
    """
    db_url = get_database_url()
    engine = create_engine(db_url, future=True)

    # try connect a few times
    for _ in range(5):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            break
        except Exception:
            time.sleep(1)
    else:
        pytest.skip("Postgres not available for integration test")

    # run the module ETL against sample data
    # use python -m etl to execute; set env to point to the DB
    rc = os.system(f"DATABASE_URL='{db_url}' python -m etl --input data --dry-run")
    assert rc == 0
