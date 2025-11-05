# ADR 0001: Use Docker Compose + Postgres + Python for local ETL

Date: 2025-11-05

Decision
--------
We will use Docker Compose to run a local Postgres instance alongside a containerized ETL worker implemented in Python 3.10.

Context
-------
- Developers need a reproducible local environment that mirrors production components.
- Postgres is a common production database and offers realistic constraints (types, transactions).
- Python offers great ecosystem support for ETL, SQLAlchemy for DB abstractions and tests.

Consequences
------------
- Pros:
  - Low onboarding friction: one command to bring Postgres + run ETL
  - Easy to replace with Kubernetes deployments or schedule in Airflow/Prefect
  - Tests can target a real database
- Cons:
  - Docker Compose is not a full orchestration solution — moving to K8s requires new manifests
  - Local compose-based testing can be slower than in-memory mocks

Trade-offs
----------
Testcontainers could be used to spin ephemeral DBs for each test, which is ideal for CI. For a focused portfolio project, Compose keeps things simple and visible.
