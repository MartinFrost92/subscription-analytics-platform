# Local Airflow with BigQuery

Use one Google Cloud project with four BigQuery datasets, all in **EU**:

| Dataset | Responsibility |
| --- | --- |
| `staging` | Ingestion and source normalization |
| `transform` | Business transformations |
| `model` | Analytical facts and dimensions |
| `utils` | Monitoring results and temporary working tables |

Airflow and its metadata PostgreSQL remain local. BigQuery becomes the target
analytical warehouse. Existing PostgreSQL volumes are preserved; no data is
migrated or deleted. The three runnable DAGs remain placeholders.

## 1. Create the Google Cloud project

In [Google Cloud Console](https://console.cloud.google.com/projectcreate),
create a project and record its **project ID**, not its display name. Link a
billing account to use full BigQuery functionality, including DML and permanent
tables. The no-billing sandbox has additional restrictions.

Create a billing budget alert in the console. Alerts notify you; they do not
stop spending. BigQuery's free storage and query allowances are separate from
any promotional trial credit. We will add query byte limits to actual pipeline
jobs when those are implemented.

## 2. Install Google Cloud CLI and sign in

Follow the [official Windows installation instructions](https://cloud.google.com/sdk/docs/install).
Open a new PowerShell terminal afterward. From the repository root:

```powershell
Set-Location container
$projectId = 'YOUR_PROJECT_ID'
gcloud init
gcloud config set project $projectId
gcloud services enable bigquery.googleapis.com --project $projectId
gcloud auth application-default login
gcloud auth application-default set-quota-project $projectId
```

`gcloud init` signs in the CLI. `application-default login` creates separate
Application Default Credentials (ADC) for Python and Airflow. Complete the
browser sign-in yourself. No downloaded service-account private key is needed.

If permissions are restricted, the runtime identity needs BigQuery Job User
on the project and BigQuery Data Editor on the four datasets. Dataset creation
requires additional `bigquery.datasets.create` permission. The project's owner
can perform initial setup. Setting the quota project also requires Service
Usage Consumer permission on that project.

## 3. Create the four datasets

Run once, in the same terminal:

```powershell
foreach ($dataset in @('staging', 'transform', 'model', 'utils')) {
    bq --project_id=$projectId --location=EU mk --dataset "${projectId}:$dataset"
    if ($LASTEXITCODE -ne 0) { throw "Dataset creation failed: $dataset" }
}
```

If a dataset already exists, inspect it instead of recreating it:
`bq show "${projectId}:staging"`. Every dataset must be in EU; do not mix EU
multi-region with individual European regions. Choose expiry policies for
temporary `utils` tables when those tables are implemented.

## 4. Make local credentials available to Airflow

Still inside `container/`:

```powershell
New-Item -ItemType Directory -Force secrets | Out-Null
Copy-Item -LiteralPath "$env:APPDATA/gcloud/application_default_credentials.json" -Destination secrets/google-adc.json
```

Set this variable in your existing `container/.env` using your editor:

```dotenv
GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID
```

Both `.env` and `secrets/` are ignored by Git. Do not paste credential JSON into
DAGs, YAML config, chat, or tracked files. Airflow mounts only this credentials
directory, read-only. After a future ADC sign-in/revocation, refresh this copy.
The Google provider and BigQuery client are already present in the official
Airflow image used by this repository.

## 5. Start and verify

For BigQuery-enabled sessions, consistently specify both files:

```powershell
docker compose -f compose.yaml -f compose.bigquery.yaml config --quiet
docker compose -f compose.yaml -f compose.bigquery.yaml up -d --wait --wait-timeout 300
docker compose -f compose.yaml -f compose.bigquery.yaml exec airflow-scheduler python /opt/airflow/scripts/verify-bigquery.py
```

The check uses Airflow's `google_cloud_default` connection and ADC. It confirms
all four datasets exist in EU, then runs `SELECT 1` with a 10 MiB billing limit.
It creates no tables and does not load data. A successful check is required
before claiming the cloud connection works.

View your datasets and tables in [BigQuery Studio](https://console.cloud.google.com/bigquery).
Select your project in the Explorer panel, then expand its datasets.

Stop the enabled stack with:

```powershell
docker compose -f compose.yaml -f compose.bigquery.yaml down
```

## Troubleshooting

- Missing credentials: complete ADC login, create `secrets/`, and copy the JSON.
  `gcloud auth login` alone does not configure ADC for client libraries.
- Permission denied: check the signed-in identity, billing linkage, BigQuery
  API, quota project, and IAM permissions. Do not solve this by putting keys in code.
- Dataset not found/location mismatch: check project ID and dataset location in
  BigQuery Studio. This setup deliberately checks all four datasets in EU.
- Airflow connection does not appear in the UI: this connection is configured
  through an environment variable; it need not appear as a metadata database row.
- Plain `docker compose up` omits BigQuery configuration. Use both `-f` options
  for BigQuery-enabled starts, restarts, and inspections.

## Sources

- [BigQuery local authentication](https://docs.cloud.google.com/bigquery/docs/authentication)
- [Airflow Google connection and ADC](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)
- [BigQuery dataset creation](https://cloud.google.com/bigquery/docs/datasets)
- [BigQuery pricing](https://cloud.google.com/bigquery/pricing)
