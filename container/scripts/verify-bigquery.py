"""Check BigQuery authentication, EU datasets, and query execution.

Does not create resources or load business data. Run inside the scheduler
after enabling compose.bigquery.yaml. Fails if any required dataset is missing.
"""
import os

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


def main() -> None:
    project = os.environ["GOOGLE_CLOUD_PROJECT"]
    location = os.environ["BIGQUERY_LOCATION"]
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, location=location)
    client = hook.get_client(project_id=project, location=location)
    try:
        for name in ("staging", "transform", "model", "utils"):
            dataset = client.get_dataset(f"{project}.{name}")
            if dataset.location.upper() != location.upper():
                raise ValueError(f"{name} is in {dataset.location}, expected {location}")
            print(f"PASS dataset {dataset.full_dataset_id}: {dataset.location}", flush=True)
        job = client.query(
            "SELECT 1 AS connection_check",
            location=location,
            job_config=bigquery.QueryJobConfig(
                use_legacy_sql=False,
                maximum_bytes_billed=10 * 1024 * 1024,
            ),
        )
        rows = list(job.result(timeout=60))
        if len(rows) != 1 or rows[0].connection_check != 1:
            raise RuntimeError("Unexpected connection-check result")
        print(f"PASS query {job.job_id}; bytes billed: {job.total_bytes_billed}", flush=True)
    finally:
        client.close()


if __name__ == "__main__":
    main()
