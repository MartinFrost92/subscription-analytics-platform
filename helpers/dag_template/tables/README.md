# Table definitions

The `tables.table.json` file contains an array of table definitions. One Airflow
task, `create_<dag_filename>_tables`, creates all tables listed in that file.
`dataset_id` and `table_id` inside each definition select the destination;
the definition filename does not determine the table name.

The DAG calls the shared operator from `helpers/plugins/` with two arguments:

```python
CreateTablesOperator(
    task_id=f"create_{DAG_FILE.stem}_tables",
    tables="tables/tables.table.json",
)
```

Airflow renders the file and schema includes relative to the DAG's folder.
The container adds `helpers/plugins/` to `PYTHONPATH` for shared operator imports.

Definitions are Jinja templates rendered at task runtime using Airflow's
context, including `params`. For example:

```jinja
[
  {
    "dataset_id": "{{ params.bigquery.dataset_id }}",
    "table_id": "dim_customer",
    "schema_fields": {% include "schemas/dim_customer.schema.json" %},
    "clustering": {"fields": ["customer_id"]}
  }
]
```

The included schema file must contain a BigQuery schema-fields JSON array.
Include paths are relative to the DAG folder. Provide the real schema before
running the task; a missing schema fails at runtime, not during DAG discovery.

Keep optional BigQuery table-resource fields here, such as `description`,
`labels`, `clustering`, and `timePartitioning` (using BigQuery API field names).
The shared connection always supplies the project; a definition cannot
override it through `tableReference`.

Existing tables are ignored (`exists_ok=True`, equivalent to the creation
operator's `if_exists="ignore"`). Changed definitions do not migrate existing
schemas. If a run partially succeeds, retrying safely skips existing tables.
