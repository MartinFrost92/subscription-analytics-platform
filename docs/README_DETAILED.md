# Subscription analytics platform

A public portfolio showcase of end-to-end subscription analytics. This initial
milestone provides local orchestration only. Ingestion, validation, dimensional
modeling, and Power BI analytics are future work.

## Architecture and structure

For the optional EU BigQuery warehouse (`staging`, `transform`, `model`, and
`utilities` datasets), follow [the BigQuery setup guide](container/BIGQUERY.md).
Local Google authentication and dataset verification are required before use.

Official `apache/airflow:3.1.7-python3.12` and `postgres:16-bookworm` images.
Six services: API server (UI and execution API), scheduler using LocalExecutor,
standalone DAG processor, one-shot metadata migration, metadata PostgreSQL,
and analytics PostgreSQL. No Redis, Celery, worker service, Kubernetes, cloud
services, or triggerer (there are no deferrable tasks).

`metadata-db` contains the `airflow` database. `analytics-db` contains separate
`warehouse` and `subscription_source` databases. Both instances use persistent
named volumes; additional volumes retain task logs and generated UI passwords.
Database ports stay internal; the UI binds to `127.0.0.1` only.

```text
container/                    Everything needed to run the local stack
  compose.yaml                Services, health checks, dependencies, volumes
  .env.example                Variable names, no credentials
  postgres/init/              First-boot database initialization
  scripts/init-env.ps1        Local secret generation
  scripts/verify.py           Discovery and execution smoke check
dags/                         Independent Airflow DAGs
  staging_dag.py              Ingestion and source normalization
  transform_dag.py            Business transformations
  model_dag.py                Analytical facts and dimensions
src/subscription_analytics/   Reusable Python implementation
helpers/                      Future reusable SQL and helper files
  plugins/utilities/ Shared Airflow operators
```

Each independent DAG contains one executable Python logging placeholder and
uses manual scheduling. There are no cross-DAG dependencies or business logic.
DAGs remain thin; reusable modules and SQL hold future implementation. Define
data contracts and validation before implementing actual pipelines.

## Prerequisites

- Windows Docker Desktop running Linux containers with the WSL 2 backend.
- Compose v2.14+ (`docker compose version`).
- At least 4 GB RAM available to Docker/WSL, preferably 8 GB.
- PowerShell, Git, and an available port 8080. No host Python required.

Run the commands below from the `container/` directory in PowerShell. No paid cloud services
are used; Docker Desktop's subscription terms still apply.

## Initialize and start

```powershell
# From the repository root:
Set-Location container
docker version
docker compose version
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/init-env.ps1
docker compose config --quiet
docker compose up -d --wait --wait-timeout 300
docker compose ps -a
```

The generator creates an ignored `.env` with random database passwords, Fernet
key, shared JWT signing secret, and API signing key; it refuses to overwrite existing secrets.
The execution policy override applies only to that PowerShell process.
`.env.example` documents variable names; blank values are deliberately unusable.
Keep DB passwords URL-safe (the generator uses hex) for the SQLAlchemy URL.
Never commit `.env`, generated passwords, or rendered Compose output. Validate
with `config --quiet` to avoid printing credentials.

`up` automatically runs metadata migration before starting Airflow. Expect
`airflow-init` to exit with code 0 and five long-running healthy services.
Initial image downloads can take several minutes. Explicit initialization,
when needed on a stopped stack:

```powershell
docker compose up -d --wait metadata-db analytics-db
docker compose run --rm airflow-init
docker compose up -d --wait --wait-timeout 300
```

Open http://localhost:8080. No sign-in is required: SimpleAuthManager's
`simple_auth_manager_all_admins` option gives local UI visitors admin access.
The UI is bound to `127.0.0.1` for this local development setup. Database
passwords and internal signing keys are still generated and kept in `.env`.
The auth volume is retained so existing local state survives configuration changes.
The analytics databases share one local owner; application roles will be
designed with actual ingestion.

In Docker Desktop, `subscription-analytics` is the Compose **project group**.
It has no container ID, image, or port of its own. Expand its row, then find
`airflow-api-server` (container `subscription-analytics-airflow-api-server-1`)
and click its published `8080:8080` port to open Airflow. The scheduler and DAG
processor do not publish UI ports. You can also open the URL above directly.

## Verify discovery and execution

```powershell
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow config get-value core executor
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler python /opt/airflow/scripts/verify.py
docker compose exec analytics-db psql -U analytics -d warehouse -c "SELECT current_database();"
docker compose exec analytics-db psql -U analytics -d subscription_source -c "SELECT current_database();"
```

Expect 3.1.7, LocalExecutor, three DAGs, and no import errors. The smoke script
waits for discovery, unpauses each DAG, triggers a unique run, then checks that
all three runs and Python task instances reach `success`. Failures and timeouts
exit nonzero. This verifies real scheduler/LocalExecutor execution through the
execution API and shared module imports. DAGs remain unpaused with no schedule.

Manual execution and inspection:

```powershell
docker compose exec airflow-scheduler airflow dags unpause staging_dag
docker compose exec airflow-scheduler airflow dags trigger staging_dag
docker compose exec airflow-scheduler airflow dags list-runs staging_dag
docker compose logs --tail 100 airflow-scheduler airflow-dag-processor airflow-api-server
docker compose exec airflow-scheduler find /opt/airflow/logs -type f
```

Task logs are also available in the UI and persist in `airflow-logs`.

## Stop, restart, and reset

```powershell
docker compose stop
docker compose start --wait
# Remove containers/network, preserving database and log volumes:
docker compose down
docker compose up -d --wait --wait-timeout 300
```

**Destructive full reset:** this deletes both database stores, metadata, run
history, logs, and UI passwords. Only use to deliberately discard local state.
Keep `.env` for the next startup.

```powershell
docker compose down --volumes
docker compose up -d --wait --wait-timeout 300
```

## Troubleshooting

- Docker pipe/access errors: start Docker Desktop, select Linux containers,
  enable WSL 2, and check `docker version`. Sandboxed tools may need explicit
  Docker access even when your terminal has it.
- Missing environment variables: run the generator once. Inspect existing
  `.env` privately for missing values. Changing `.env` passwords does not update
  existing PostgreSQL volumes; preserve secrets across restarts.
- Port conflict: set `AIRFLOW_PORT=8081` in `.env`, rerun
  `docker compose up -d --wait --wait-timeout 300`, then use that port.
- Startup failure: run `docker compose ps -a` and
  `docker compose logs --tail 100 airflow-init metadata-db analytics-db`, then
  inspect Airflow logs with the command above. Exit 137 can indicate insufficient
  memory; increase Docker/WSL resources and retry `up`.
- DAG discovery: new files are scanned every 10 seconds in this local setup;
  allow additional time for parsing. Inspect `list-import-errors` and DAG
  processor logs. Confirm Docker Desktop can read the repository bind mounts.
- Task failures: inspect task logs and scheduler/API logs. Verify LocalExecutor
  and the execution API URL with `airflow config get-value`. All Airflow services
  must share JWT, API signing, and Fernet secrets.
- Missing source database: initialization SQL runs only on an empty analytics
  volume. For an existing volume, run
  `docker compose exec analytics-db psql -U analytics -d warehouse -c "CREATE DATABASE subscription_source;"`.
  Do not delete volumes merely to rerun initialization.
- Windows script policy: use the documented process-scoped command; no permanent
  policy change is needed. From WSL, use `powershell.exe` for that command.

## Tested locally

Validated on 2026-10-26 using Docker Desktop's Linux engine on Windows:

- `docker compose config --quiet` passed; five services became healthy and
  metadata initialization exited successfully.
- Airflow reported version 3.1.7 and LocalExecutor; all three DAGs were
  discovered with no import errors.
- Each DAG and its Python task succeeded through scheduler execution, including
  after container recreation. Final smoke run:
  `smoke_a5bfa3a968524f65b559ecb801bcfa3b`.
- Both analytics databases accepted queries. Earlier successful DAG runs
  persisted across recreation.
- API health reported healthy metadata, scheduler, and DAG processor. The
  triggerer status is null because no triggerer is required or deployed.

Initial startup exposed root-owned authentication volume permissions; the
initialization service now fixes volume ownership before migration. Windows
blocked direct script execution; the documented process-scoped invocation
worked. No unresolved startup or DAG execution errors remained in these checks.
Business pipelines and data validation have not been implemented or tested.

## References

- [Official Airflow Docker guide](https://airflow.apache.org/docs/apache-airflow/3.1.7/howto/docker-compose/index.html)
- [Airflow configuration](https://airflow.apache.org/docs/apache-airflow/3.1.7/configurations-ref.html)
- [Official PostgreSQL image](https://hub.docker.com/_/postgres)
