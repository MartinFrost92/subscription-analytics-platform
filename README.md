# Subscription analytics platform

Subscription analytics development with Airflow 3, PostgreSQL, and Google Cloud.

## Architecture

Docker Compose runs Airflow, metadata and analytics PostgreSQL, and a synthetic
source API with dedicated PostgreSQL. The BigQuery overlay enables Google Cloud access.

## Prerequisites

- Git and Bash (Git Bash on Windows).
- Docker running Linux containers, Docker Compose v2.14+, and at least 4 GB RAM
  available to Docker (8 GB recommended).
- Available localhost ports 8080 and 8000.
- On Windows, install Python 3.12 (available from the Microsoft Store) for the
  host command-line tools. Airflow itself runs with Python inside Docker.
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) and access to the platform's Google Cloud project.
  Reopen your terminal after installation.

## Windows setup

1. Open **PowerShell as Administrator** and run:

   ```powershell
   wsl --install
   ```

2. Restart Windows when prompted. Open Ubuntu to finish its first-time setup
   and create a Linux username and password.

3. Install **Python 3.12** from the Microsoft Store and reopen your terminal.

4. Install and open **Docker Desktop**, using the WSL 2 backend and Linux
   containers. If running setup inside Ubuntu, enable Ubuntu under
   **Settings > Resources > WSL Integration** and apply the change.

## Run

Authenticate before running setup:

```bash
gcloud auth application-default login
```

```bash
git clone https://github.com/MartinFrost92/subscription-analytics-platform.git
cd subscription-analytics-platform
bash scripts/setup.sh
```

Setup checks Google Cloud CLI and authentication before starting services,
copies ADC credentials automatically, and enables the BigQuery connection.
It preserves existing secrets and database volumes and completes only after
Google Cloud access checks pass.

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
