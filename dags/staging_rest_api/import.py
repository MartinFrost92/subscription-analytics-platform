"""Extract endpoints from the manually maintained REST configuration."""
import json
from pathlib import Path

import pendulum
import yaml
from airflow.sdk import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from utilities.bigquery import CreateTablesOperator
from utilities.rest_api import RestAPIToGCSOperator

DAG_DIR = Path(__file__).resolve().parent
CONFIG_PATH = DAG_DIR / 'config' / 'config.yml'
GCS_BUCKET = 'subscription-analytics-raw'
with CONFIG_PATH.open(encoding='utf-8') as stream:
    config = yaml.safe_load(stream)
schemas = {
    endpoint['name']: json.loads(
        (DAG_DIR / 'schemas' / f"{endpoint['name']}.schema.json").read_text(encoding='utf-8')
    )
    for endpoint in config['end_points']
}

defaults = config['dag_defaults'].copy()
defaults['start_date'] = pendulum.parse(defaults['start_date'])

with DAG(dag_id='staging_rest_api',
         params={**config, 'gcs_bucket': GCS_BUCKET},
         **defaults) as dag:
         
    create_tables = CreateTablesOperator(
        task_id='create_staging_tables',
        tables="tables/tables.table.json",
    )

    extract_tasks = {}
    load_tasks = {}
    for endpoint in config['end_points']:
        name = endpoint['name']
        extract_tasks[endpoint['name']] = RestAPIToGCSOperator(
            task_id=f"extract_{endpoint['name']}",
            connection_id=config['connection_id'],
            config_path=str(CONFIG_PATH),
            endpoint_name=endpoint['name'],
            local_output_dir='/opt/airflow/staging',
            max_workers=100,
            requests_per_second=1000,
            gcs_bucket='{{ params.gcs_bucket }}',
            gcs_prefix='staging/',
        )

        load_tasks[name] = GCSToBigQueryOperator(
            task_id=f'load_{name}',
            bucket='{{ params.gcs_bucket }}',
            source_objects=[
                "{{ ti.xcom_pull(task_ids='" + extract_tasks[name].task_id + "')[0].split('/', 3)[3] }}"
            ],
            destination_project_dataset_table=f"{config['project_id']}.{config['datasets']['staging']}.{name}",
            source_format='NEWLINE_DELIMITED_JSON',
            schema_fields=schemas[name],
            autodetect=False,
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='google_cloud_default',
            location='EU',
            deferrable=False,
        )
        create_tables >> extract_tasks[name] >> load_tasks[name]
