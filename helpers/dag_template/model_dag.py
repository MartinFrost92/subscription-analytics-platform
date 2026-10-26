"""Analytical fact and dimension construction; no business logic yet."""
from pathlib import Path

import yaml

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from subscription_analytics.placeholders import placeholder

# Both the checkout and container mount root contain dags/ and src/.
# Keep resource paths relative to DAG_DIR when copying this template.
DAG_FILE = Path(__file__).resolve()
DAG_DIR = DAG_FILE.parent
PROJECT_ROOT = next(
    parent
    for parent in DAG_FILE.parents
    if (parent / "dags").is_dir() and (parent / "src").is_dir()
)
DAG_ID = "_".join(DAG_FILE.relative_to(PROJECT_ROOT).with_suffix("").parts)

with (DAG_DIR / "config" / "config.yml").open(encoding="utf-8") as config_file:
    config = yaml.safe_load(config_file)

with DAG(
    dag_id=DAG_ID,
    params=config,
    **config["dag"],
) as dag:
    PythonOperator(
        python_callable=placeholder,
        op_kwargs={"stage": "{{ params.stage }}"},
        **config["task"],
    )
