"""Analytical fact and dimension construction; no business logic yet."""
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from subscription_analytics.placeholders import placeholder

with DAG(
    dag_id="model_dag",
    description="Build analytical facts and dimensions (placeholder).",
    schedule=None,
    catchup=False,
    tags=["model"],
) as dag:
    PythonOperator(task_id="model_placeholder", python_callable=placeholder, op_kwargs={"stage": "model"})
