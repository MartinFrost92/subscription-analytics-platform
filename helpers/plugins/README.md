# Shared Airflow operators

The container adds this directory to `PYTHONPATH`. Import shared operators
from the `utilities` package; no plugin registration is required.

## Extracting REST APIs

`RestAPIToGCSOperator` uses `HttpHook` authentication from `connection_id`,
including connections resolved through the configured Secret Manager backend.
Connection extras can supply API headers. It writes raw JSONL to
`<gcs_prefix>/<endpoint name>/<interval start>_<interval end>.jsonl` and returns
the GCS URIs. Retrying an interval overwrites its objects and local files.
HTTP and parsing errors propagate to Airflow; configure task retries in the DAG.
The former `max_request_retries` and `retryable_status_codes` arguments were removed.

Endpoint configuration supports inline records or a collection of IDs with
`detail_path: customers/{id}`. Use `id_field` for object IDs and `records_path`
for nested collections; both accept dotted paths including array indexes.
Detail requests run in bounded batches using `max_workers`, with a shared
`requests_per_second` limit per extraction task. Independent tasks have separate limits.

Responses default to JSON. Set `response_format: xml` at the config root or on
an endpoint to parse XML before selecting records and writing JSONL:

```yaml
end_points:
  - name: rates
    path: rates.xml
    response_format: xml
    records_path: rates.rate
```

For `<rates><rate code="EUR"><value>7.46</value></rate></rates>`, this writes
`{"@code":"EUR","value":"7.46"}`. XML attributes use `@`, text alongside
attributes/children uses `#text`, and repeated tags become lists. Namespace
prefixes are removed; paths use local tag names. Values remain strings.
Single selected XML records become one-item lists; an empty selected element
produces no records. Missing configured paths still raise parsing errors.
The root element remains in parsed responses, including detail responses.
Endpoint settings override the root response format. Both collection and
detail requests use the endpoint's format; no extra packages are needed.

```yaml
connection_id: synthetic_api
extraction_settings:
  max_workers: 10
  requests_per_second: 20
end_points:
  - name: invoices
    path: invoices
    request: GET
    detail_path: invoices/{id}
    expected_rows_header: X-Total-Count
    pagination:
      strategy: page
      identifier: page
      params: {page_size: 1000}
```

Pagination supports `page` (default start 1), `offset` (default start 0,
required `step`), `cursor` (`identifier` and `cursor_path`), and `url`
(`next_url_path`). Page/offset pagination ends on an empty page unless
`has_more_path` specifies a boolean continuation field. Cursor/URL pagination
ends on a missing or null continuation. Repeated continuations and `max_pages`
prevent infinite loops. These guards do not reconcile downloaded counts.
Pagination and detail URLs must remain on the connection origin or an explicit
`approved_hosts` origin; HTTP redirects are refused.

For totals reported in a JSON response, configure `expected_rows_path` instead
of `expected_rows_header`. The first collection response supplies the total;
it is never computed from downloaded records. Unconfigured totals, absent
headers, and explicit JSON null totals yield `expected_rows: null`.
A configured JSON path that does not exist raises a parsing error.

Extraction pushes a list of `{name, expected_rows}` objects to the
`extraction_metadata` XCom key. Create one
`ConsolidateExtractionMetadataOperator(extraction_task_ids=..., gcs_bucket=...)`
per DAG and set each extraction task as its upstream dependency.
It writes one JSONL file under
`utilities/<data_interval_end as ts_nodash>_<dag_id>_metadata.json`,
containing one row per endpoint. The `.json` extension follows the metadata
naming convention; load it using BigQuery's `NEWLINE_DELIMITED_JSON` format
with `name STRING` and nullable `expected_rows INTEGER`.
The consolidation task returns the metadata GCS URI and overwrites the same
object on reruns. Extraction and consolidation do not perform reconciliation,
BigQuery transformations, or promotion to staging.

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
table's contents and schema using `WRITE_TRUNCATE`. The query output defines
the replacement schema; existing constraints are removed. The destination
dataset is resolved from `tables/tables.table.json`.
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
