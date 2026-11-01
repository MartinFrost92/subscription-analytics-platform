"""Prepare the existing platform project with the same ADC identity as Airflow."""
import os
import json
import time

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


def main():
    project = os.environ["GOOGLE_CLOUD_PROJECT"]
    location = os.environ["BIGQUERY_LOCATION"]
    target = os.environ["GOOGLE_IMPERSONATED_SERVICE_ACCOUNT"]
    with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], encoding="utf-8") as source:
        adc = json.load(source)
    expected = f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{target}:generateAccessToken"
    if adc.get("type") != "impersonated_service_account" or adc.get("service_account_impersonation_url") != expected:
        raise RuntimeError(f"ADC must impersonate {target}; rerun bash scripts/setup.sh.")
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    # Reuse ADC rather than a potentially different gcloud CLI account.
    with AuthorizedSession(credentials) as session:
        base = "https://serviceusage.googleapis.com/v1/"
        service = f"projects/{project}/services/bigquery.googleapis.com"
        response = session.get(base + service, timeout=60)
        if response.status_code == 403:
            # Runtime access does not require serviceusage.services.get.
            # Let the dataset/query calls below establish whether BigQuery works.
            print("Cannot inspect API settings; checking BigQuery access directly.", flush=True)
        elif not response.ok:
            raise RuntimeError(f"Cannot inspect BigQuery API settings: {response.text}")
        elif response.json().get("state") != "ENABLED":
            response = session.post(base + service + ":enable", json={}, timeout=60)
            if not response.ok:
                raise RuntimeError(
                    f"BigQuery API is disabled and activation failed: {response.text}. "
                    "A project administrator can enable bigquery.googleapis.com."
                )
            operation = response.json()
            deadline = time.monotonic() + 180
            while not operation.get("done"):
                if time.monotonic() >= deadline:
                    raise RuntimeError("BigQuery API activation timed out; rerun setup.")
                time.sleep(2)
                response = session.get(base + operation["name"], timeout=60)
                response.raise_for_status()
                operation = response.json()
            if "error" in operation:
                raise RuntimeError(operation["error"])
    with bigquery.Client(project=project, credentials=credentials, location=location) as client:
        for name in ("staging", "transform", "model", "utilities"):
            dataset_id = f"{project}.{name}"
            try:
                dataset = client.get_dataset(dataset_id)
            except NotFound:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = location
                client.create_dataset(dataset, exists_ok=True)
                dataset = client.get_dataset(dataset_id)
            if dataset.location.upper() != location.upper():
                raise RuntimeError(f"{dataset_id} is in {dataset.location}; expected {location}. Existing datasets are preserved.")
            print(f"PASS dataset {dataset_id}: {dataset.location}", flush=True)
        list(client.query("SELECT 1", location=location).result(timeout=60))
        print("PASS BigQuery query permission", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(
            f"Google Cloud preparation failed: {exc}\n"
            "Your personal Google account needs Service Account Token Creator on airflow-writer. "
            "Ask the project administrator to check the impersonated service account: Service Usage Consumer; "
            "Service Usage Admin if API activation is needed; BigQuery Job User on the project; "
            "BigQuery Data Editor on existing datasets, and bigquery.datasets.create if datasets are missing. "
            "Setup cannot grant permissions to an account that lacks IAM administration rights."
        ) from exc
