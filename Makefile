PYTHON ?= python3

.PHONY: build up run test lint

build:
	docker compose build

up:
	docker compose up --build

run:
	docker compose run --rm etl

test:
	pytest -q

lint:
	ruff check src tests

migrate:
	# Apply SQL migration to the running postgres (requires psql in PATH)
	psql "postgresql://etl_user:etl_pass@localhost:5432/etl_db" -f migrations/0001_create_etl_results.sql

verify:
	# Run ETL (non-dry) and show results
	docker-compose run --rm etl && \
	psql "postgresql://etl_user:etl_pass@localhost:5432/etl_db" -c "select * from etl.results order by created_at desc limit 10;"
