"""Extract endpoints from the manually maintained REST configuration."""
import json
from pathlib import Path

import pendulum
import yaml
from airflow.sdk import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from utilities.bigquery import CreateTablesOperator, DeltaLoadTableOperator, METADATA_SCHEMA
from utilities.validation import BigQueryDataValidationOperator
from utilities.rest_api import RestAPIToGCSOperator, ConsolidateExtractionMetadataOperator

DAG_DIR = Path(__file__).resolve().parent
CONFIG_PATH = DAG_DIR / 'config' / 'config.yml'
GCS_BUCKET = 'subscription-analytics-raw'
with CONFIG_PATH.open(encoding='utf-8') as stream:
    config = yaml.safe_load(stream)
tmp_schemas = {
    endpoint['name']: json.loads(
        (DAG_DIR / 'schemas' / f"tmp_{endpoint['name']}.schema.json").read_text(encoding='utf-8')
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
            **config.get('extraction_settings', {}),
            gcs_bucket='{{ params.gcs_bucket }}',
            gcs_prefix='staging/',
        )

        load_tasks[name] = GCSToBigQueryOperator(
            task_id=f'load_{name}',
            bucket='{{ params.gcs_bucket }}',
            source_objects=[
                "{{ ti.xcom_pull(task_ids='" + extract_tasks[name].task_id + "')[0].split('/', 3)[3] }}"
            ],
            destination_project_dataset_table=(
                f"{config['project_id']}.{config['datasets']['utilities']}."
                f"{{{{ data_interval_end | ts_nodash }}}}_{dag.dag_id}_{name}"
            ),
            source_format='NEWLINE_DELIMITED_JSON',
            schema_fields=tmp_schemas[name],
            autodetect=False,
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id='google_cloud_default',
            location='EU',
            deferrable=False,
        )
        create_tables >> extract_tasks[name] >> load_tasks[name]

    consolidate_metadata = ConsolidateExtractionMetadataOperator(
        task_id='consolidate_extraction_metadata',
        extraction_task_ids=[task.task_id for task in extract_tasks.values()],
        gcs_bucket='{{ params.gcs_bucket }}',
        name=config['name'],
    )
    for extract_task in extract_tasks.values():
        extract_task >> consolidate_metadata

    load_metadata = GCSToBigQueryOperator(
        task_id='load_metadata',
        bucket='{{ params.gcs_bucket }}',
        source_objects=[
            "{{ ti.xcom_pull(task_ids='" + consolidate_metadata.task_id + "').split('/', 3)[3] }}"
        ],
        destination_project_dataset_table=(
            f"{config['project_id']}.{config['datasets']['utilities']}."
            f"{{{{ data_interval_end | ts_nodash }}}}_{dag.dag_id}_{config['name']}_metadata"
        ),
        source_format='NEWLINE_DELIMITED_JSON',
        schema_fields=METADATA_SCHEMA,
        autodetect=False,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='google_cloud_default',
        location='EU',
        deferrable=False,
    )
    consolidate_metadata >> load_metadata

    validate_data = BigQueryDataValidationOperator(
        task_id='validate_data',
        sql='queries/monitoring/data_validation.sql',
        project_id='{{ params.project_id }}',
        gcp_conn_id='google_cloud_default',
        location='EU',
    )
    load_metadata >> validate_data
    for load_task in load_tasks.values():
        load_task >> validate_data

    table_tasks = {}
    for endpoint in config['end_points']:
        name = endpoint['name']
        table_tasks[name] = DeltaLoadTableOperator(
            task_id=f'delta_load_{name}',
            table_id=f'merge_{name}',
        )
        validate_data >> table_tasks[name]
