# Build Local ETL Pipeline (CSV → Transform → Postgres)

Small, opinionated example project that demonstrates a local ETL pipeline implemented in Python 3.10+, designed to be run locally with Docker Compose.

Goal
----
Read CSV files from `data/`, normalize dates to ISO 8601, aggregate amount by day, validate rows, and load results into Postgres table `etl.results`.

Quickstart (one-command run)
----------------------------
Start services (Postgres) and build the ETL image:

```bash
docker compose up --build -d
```

Run the ETL against `data/`:

```bash
docker compose run --rm etl
```

Check results (example):

```bash
psql "postgresql://etl_user:etl_pass@localhost:5432/etl_db" -c "select * from etl.results limit 10;"
```

Project layout
--------------

Key files and folders:

- `data/` - sample CSV input(s)
- `src/etl/` - implementation (extract, transform, load, db, config, logger)
- `tests/` - unit and integration tests
- `docker compose.yml` - Postgres + ETL service
- `Dockerfile` - ETL image
- `adr/` - architecture decisions

Design & architecture (short)
-----------------------------
This repository intentionally stays small and opinionated:

- Python 3.10 for portability and typing
- Docker Compose for local reproducibility and onboarding
- Postgres as a realistic target DB
- SQLAlchemy for DB access and testability
- Structured logging and clear separation of concerns (extract/transform/load)

Running tests
-------------
Unit tests (fast):

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
# Recommended: install runtime + dev dependencies
python -m pip install -r requirements.txt -r requirements-dev.txt
# OR install the package in editable mode and dev deps:
# python -m pip install -e .
pytest -q
```

Integration test (starts a Postgres service in CI or use `docker compose up -d` locally):

```bash
# start postgres
docker compose up --build -d postgres
# in another shell
pytest tests/integration -q
```

How to extend / productionize (short)
-----------------------------------

- Move from Docker Compose to Kubernetes/Helm or run ETL as a scheduled job in Airflow/Prefect.
- Use managed Postgres (RDS/Azure) and store secrets in vault/secret manager.
- Add metrics exporter (Prometheus) and logs to a central store.

Tech-lead notes
---------------

1. Observability: logs are structured; for production I'd add OpenTelemetry + Prometheus metrics. Alerting on job failures and row-level error rates is essential.
2. Testing: add contract tests and e2e tests; add testcontainers for isolated DB testing locally.
3. Security: don't store secrets in repo; use env or secret stores. Run DB migrations with a tool like Alembic.

TODO / Next steps (prioritized)
------------------------------
1. Add Alembic migrations and CI schema check (high ROI).
2. Add testcontainers-based integration tests to avoid depending on external compose in tests.
3. Add metrics and health endpoints; wire into CI for smoke tests.

Polish completed in this repo
----------------------------

This repository includes a simple SQL migration (`migrations/0001_create_etl_results.sql`), a `Makefile` target `make migrate` to apply it (requires `psql`), and a `scripts/verify_etl_and_query.sh` helper that starts Postgres, applies migration, runs the ETL and prints sample rows.

Running migrations
------------------
Apply the provided SQL migration directly with `psql` or via the Makefile:

```bash
# using make (requires psql available locally and postgres running on localhost)
make migrate

# or with psql directly
psql "postgresql://etl_user:etl_pass@localhost:5432/etl_db" -f migrations/0001_create_etl_results.sql
```

Observability & production notes
--------------------------------
- Logs: structured plain-text logs are emitted to stdout. In production, send logs to a log aggregator (Elastic, Datadog, CloudWatch) and use JSON formatting.
- Metrics: add Prometheus client instrumentation for job run duration, row counts, and error counts. Export to a pushgateway or scrape endpoint.
- Health: add a small HTTP health endpoint or expose probe status for container orchestration.

Migration path to Kubernetes / Airflow / Prefect
------------------------------------------------
- Kubernetes: containerize ETL (already provided) and run as a CronJob or Job; manage Postgres via a managed instance or StatefulSet + PVC.
- Airflow/Prefect: wrap `python -m etl` as a task and schedule; use Secrets backend for DB credentials.

Sample SQL queries
------------------
Use these to inspect the loaded aggregates:

```sql
-- recent aggregates
select * from etl.results order by created_at desc limit 50;

-- daily totals for a date range
select date, sum(total_amount) from etl.results
where date between '2025-01-01' and '2025-12-31'
group by date order by date;
```

License
-------
MIT — see `LICENSE`.
