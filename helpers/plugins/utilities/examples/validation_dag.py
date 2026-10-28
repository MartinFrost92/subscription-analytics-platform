"""Copy this DAG and row_count.sql together into a DAG folder to use them."""
import pendulum

from airflow.sdk import DAG
from utilities.validation import BigQueryDataValidationOperator


with DAG(
    dag_id="example_bigquery_validation",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    params={
        "project_id": "your-project",
        "output_dataset": "transform",
        "output_table": "customers",
        "source_dataset": "staging",
        "source_table": "customers",
    },
):
    BigQueryDataValidationOperator(
        task_id="validate_customer_count",
        sql="row_count.sql",
        project_id="{{ params.project_id }}",
    )
