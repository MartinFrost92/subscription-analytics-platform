"""Ingestion and source normalization; no business logic yet."""
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from subscription_analytics.placeholders import placeholder

with DAG(
    dag_id="staging_dag",
    description="Ingest and normalize subscription source data (placeholder).",
    schedule=None,
    catchup=False,
    tags=["staging"],
) as dag:
    PythonOperator(task_id="staging_placeholder", python_callable=placeholder, op_kwargs={"stage": "staging"})
