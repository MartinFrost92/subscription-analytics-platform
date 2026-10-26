"""Business transformations; no business logic yet."""
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from subscription_analytics.placeholders import placeholder

with DAG(
    dag_id="transform_dag",
    description="Apply business transformations (placeholder).",
    schedule=None,
    catchup=False,
    tags=["transform"],
) as dag:
    PythonOperator(task_id="transform_placeholder", python_callable=placeholder, op_kwargs={"stage": "transform"})
