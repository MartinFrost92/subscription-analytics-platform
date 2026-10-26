# Shared Airflow operators

The container adds this directory to `PYTHONPATH`. Import shared operators
from the `subscription_operators` package; no plugin registration is required.

## Waiting for upstream DAGs

`WaitForDagSensor` accepts `task_id`, `external_dag_id`, and `time_delta`.
Keep the two dependency settings in each DAG's config:

```yaml
wait_for_dags:
  - external_dag_id: transform_blabla_something
    time_delta: {hours: 1, minutes: 30}
  - external_dag_id: transform_another_example
    time_delta: {hours: 0, minutes: 0}
```

Loop through the entries to create one sensor per upstream DAG. All sensors
must succeed before downstream table creation. Use `wait_for_dags: []` when
the DAG has no upstream dependency.

The shared sensor checks whole-DAG success every 60 seconds, releases its
worker slot between checks, fails if the matching upstream run fails, and
times out after 15 minutes. Missing DAGs continue waiting until timeout.
No triggerer is needed.

Offsets subtract from the current run's **logical date**, not wall-clock time.
An offset of 1 hour 30 minutes targets the upstream logical date exactly
90 minutes earlier. It does not choose the latest success in that time window.
Schedules/logical dates must align; manual test runs need an appropriate
logical date. Fictional upstream names are placeholders and cannot satisfy
the dependency.

## Writing table data

`FullLoadTableOperator(task_id=..., table_id=...)` reads
`queries/production/<table_id>.sql`. Its SELECT results replace the existing
table's contents using `WRITE_TRUNCATE_DATA`, preserving its schema and
constraints. The destination dataset is resolved from `tables/tables.table.json`.
`CREATE_NEVER` requires table creation to have succeeded first.

`DeltaLoadTableOperator(task_id=..., table_id=...)` reads the same SQL path
and executes the SQL as written. Incremental SQL such as MERGE must specify
its own destination. The operator does not add truncation or append behavior.

Both use `google_cloud_default`, the project in DAG `params.project_id`,
and the central EU location. SQL is rendered with Airflow's runtime context,
including `params` and `data_interval_end`. They extend Airflow's
`BigQueryInsertJobOperator` and wait for the query job to finish.

Configure one entry per table under `full_load_tables` or `delta_load_tables`:

```yaml
full_load_tables:
  - name: dim_date
    depends_on: null
  - name: dim_customer
    depends_on: null
delta_load_tables:
  - name: fact_sales
    depends_on: [dim_date, dim_customer]
```

The DAG builds one task per table, gates all loads on table creation, and wires
`depends_on` to other configured table names. Independent tables can run in
parallel; fact_sales waits for both dimensions. Names must be unique across
the two lists, and dependencies must reference configured tables without cycles.

BigQuery MERGE requires billing to be enabled; no-billing sandbox projects
reject DML even when the generated dataset is tiny. Pipeline execution can be
tested only after that restriction is resolved and upstream sensor runs match.
