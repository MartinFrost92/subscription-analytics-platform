# Subscription analytics platform

Subscription analytics development with Airflow 3, PostgreSQL, and optional
Google Cloud access. DAGs currently contain placeholder tasks.

## Architecture

Docker Compose runs Airflow, metadata and analytics PostgreSQL, and a synthetic
source API with dedicated PostgreSQL. An optional overlay enables BigQuery.

## Prerequisites

- Git and Bash (Git Bash on Windows).
- Docker running Linux containers, Docker Compose v2.14+, and at least 4 GB RAM
  available to Docker (8 GB recommended).
- Available localhost ports 8080 and 8000. No host Python required.

## Run

```bash
git clone https://github.com/MartinFrost92/subscription-analytics-platform.git
cd subscription-analytics-platform
bash scripts/setup.sh
```

Setup preserves existing secrets and database volumes. Google Cloud requires
manual authentication; setup reports remaining configuration.

## Airflow

Open http://localhost:8080; no sign-in is required. If customized, use
`AIRFLOW_PORT` from `container/.env`.

See [detailed documentation](docs/README_DETAILED.md) for verification,
troubleshooting, and the [Google Cloud setup guide](container/BIGQUERY.md).

The synthetic source API has its own persistent PostgreSQL service. Browse
http://localhost:8000/docs; Airflow can reach `http://source-api:8000`.
Collections return IDs ([invoice pagination](source_api/README.md)); details default to 100 ms latency
(`SOURCE_DETAIL_LATENCY_MS` in `container/.env`). Invoice periods are end-exclusive;
amounts are EUR. Data seeds once with a fixed seed and 2026-01-01 cutoff.
Run checks: `docker compose -f container/compose.yaml exec source-api python -m unittest discover -s tests`.
