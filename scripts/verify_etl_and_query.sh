#!/usr/bin/env bash
# Simple helper to run ETL (non-dry) and query Postgres for verification.
set -euo pipefail

# Ensure postgres is up (assumes docker-compose is configured as in this repo)
docker-compose up -d postgres

# wait for postgres
echo "Waiting for Postgres to be ready..."
until docker-compose exec -T postgres pg_isready -U etl_user -d etl_db >/dev/null 2>&1; do
  sleep 1
done

echo "Applying migration (if needed)"
psql "postgresql://etl_user:etl_pass@localhost:5432/etl_db" -f migrations/0001_create_etl_results.sql

echo "Running ETL"
docker-compose run --rm etl

echo "Querying results"
psql "postgresql://etl_user:etl_pass@localhost:5432/etl_db" -c "select * from etl.results order by created_at desc limit 20;"
