"""Create all tables from one DAG-relative, templated definition file."""
import json
import os
from pathlib import Path

from airflow.sdk import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


METADATA_SCHEMA = json.loads(
    (Path(__file__).parent / 'schemas' / 'metadata.schema.json').read_text(encoding='utf-8')
)


class CreateTablesOperator(BaseOperator):
    """Render table definitions and schema includes, then create missing tables."""

    template_fields = ("tables",)
    template_ext = (".json",)
    template_fields_renderers = {"tables": "json"}

    def __init__(self, *, tables: str, **kwargs):
        super().__init__(**kwargs)
        self.tables = tables

    def execute(self, context):
        definitions = json.loads(self.tables) if isinstance(self.tables, str) else self.tables
        if not isinstance(definitions, list):
            raise ValueError("The tables file must contain an array of table definitions")
        for definition in definitions:
            if not isinstance(definition, dict):
                raise ValueError("Each table definition must be an object")
            for key in ("dataset_id", "table_id", "schema_fields"):
                if key not in definition:
                    raise ValueError(f"Table definition is missing {key}")
            if not isinstance(definition["schema_fields"], list):
                raise ValueError("schema_fields must be an array")

        if not definitions:
            self.log.info("No tables to create")
            return

        location = os.environ.get("BIGQUERY_LOCATION", "EU")
        hook = BigQueryHook(
            gcp_conn_id="google_cloud_default", location=location, use_legacy_sql=False
        )
        project_id = hook.project_id
        if not project_id:
            raise ValueError("The shared Google connection must define the platform project")

        for definition in definitions:
            resource = definition.copy()
            dataset_id = resource.pop("dataset_id")
            table_id = resource.pop("table_id")
            schema_fields = resource.pop("schema_fields")
            resource["tableReference"] = {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            }
            hook.create_table(
                dataset_id=dataset_id,
                table_id=table_id,
                schema_fields=schema_fields,
                table_resource=resource,
                project_id=project_id,
                location=location,
                exists_ok=True,
            )
            self.log.info("Ensured table %s.%s.%s exists", project_id, dataset_id, table_id)


class _SqlTableOperator(BigQueryInsertJobOperator):
    """Load SQL by table name using the shared connection and DAG project."""

    def __init__(self, *, table_id: str, **kwargs):
        self.table_id = table_id
        super().__init__(
            configuration={
                "query": {
                    "query": "{% include 'queries/production/" + table_id + ".sql' %}",
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="google_cloud_default",
            project_id="{{ params.project_id }}",
            location=os.environ.get("BIGQUERY_LOCATION", "EU"),
            deferrable=False,
            **kwargs,
        )


class FullLoadTableOperator(_SqlTableOperator):
    """Replace table contents and schema with the query output."""

    template_fields = (*_SqlTableOperator.template_fields, "tables", "dataset_id")
    template_fields_renderers = {
        **_SqlTableOperator.template_fields_renderers,
        "tables": "json",
    }

    def __init__(self, *, table_id: str, dataset_id=None, **kwargs):
        super().__init__(table_id=table_id, **kwargs)
        self.dataset_id = dataset_id
        self.tables = "tables/tables.table.json"
        self.configuration["query"].update(
            writeDisposition="WRITE_TRUNCATE",
            createDisposition="CREATE_IF_NEEDED" if dataset_id else "CREATE_NEVER",
        )

    def execute(self, context):
        dataset_id = self.dataset_id
        if dataset_id is None:
            definitions = json.loads(self.tables) if isinstance(self.tables, str) else self.tables
            matches = [table for table in definitions if table["table_id"] == self.table_id]
            if len(matches) != 1:
                raise ValueError(f"Expected exactly one table definition for {self.table_id}")
            dataset_id = matches[0]["dataset_id"]
        self.configuration["query"]["destinationTable"] = {
            "projectId": self.project_id,
            "datasetId": dataset_id,
            "tableId": self.table_id,
        }
        return super().execute(context)


class DeltaLoadTableOperator(_SqlTableOperator):
    """Execute incremental SQL as written; SQL defines its MERGE destination."""
