"""Transform staged Nationalbanken rates into currency conversion pairs."""
from pathlib import Path

import pendulum
import yaml

from airflow.sdk import DAG
from utilities.dag import get_dag_id
from utilities.bigquery import (
    CreateTablesOperator,
    DeltaLoadTableOperator,
    FullLoadTableOperator,
)
from utilities.sensors import WaitForDagSensor
from utilities.validation import BigQueryDataValidationOperator

# Keep the DAG ID consistent with its location under dags/.
# Keep resource paths relative to DAG_DIR when copying this template.
DAG_FILE = Path(__file__).resolve()
DAG_DIR = DAG_FILE.parent
DAG_ID = get_dag_id(__file__)

with (DAG_DIR / "config" / "config.yml").open(encoding="utf-8") as config_file:
    config = yaml.safe_load(config_file)

dag_defaults = config["dag_defaults"].copy()
dag_defaults["start_date"] = pendulum.parse(dag_defaults["start_date"])

with DAG(
    dag_id=DAG_ID,
    params=config,
    **dag_defaults,
) as dag:

    wait_for_dags = []
    for dependency in config["wait_for_dags"]:
        wait_for_dags.append(
            WaitForDagSensor(
                task_id=f"wait_for_{dependency['external_dag_id']}",
                **dependency,
            )
        )

    create_tables = CreateTablesOperator(
        task_id=f"create_{DAG_FILE.stem}_tables",
        tables="tables/tables.table.json",
    )

    wait_for_dags >> create_tables

    tmp_tasks = {}
    for table in config["delta_load_tables"]:
        tmp_tasks[table["name"]] = FullLoadTableOperator(
            task_id=f"tmp_{table['name']}",
            table_id=f"tmp_{table['name']}",
            dataset_id=config["datasets"]["utilities"],
        )

    table_tasks = {}
    for table in config["delta_load_tables"]:
        table_tasks[table["name"]] = DeltaLoadTableOperator(
            task_id=f"delta_load_{table['name']}",
            table_id=table["name"],
        )

    for table in config["delta_load_tables"]:
        name = table["name"]
        create_tables >> tmp_tasks[name] >> table_tasks[name]

        for dependency in table.get("depends_on") or []:
            table_tasks[dependency] >> tmp_tasks[name]

    validate_data = BigQueryDataValidationOperator(
        task_id="validate_data",
        sql="queries/monitoring/data_validation.sql",
        tolerance="0",
        project_id="{{ params.project_id }}",
    )
    for table_task in table_tasks.values():
        table_task >> validate_data
