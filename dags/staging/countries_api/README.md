# Country enrichment

This DAG runs at 05:00 UTC and waits for the matching
`staging_synthetic_subscription_api_import` run to succeed.

It registers distinct customer country codes in `staging.countries`, leaving
existing rows unchanged. New rows start with `is_checked = FALSE`. The SQL
input query selects unchecked codes, and each API request sends `q=<country_id>`
to `/countries/v5/code`. `records_path: data.objects.0` selects the country object.

The `countries_api` Airflow connection supplies the base URL and Bearer header
through Secret Manager; credentials do not belong in the DAG config.

The temporary schema covers all top-level fields in the supplied country
response. Nested objects and arrays use BigQuery JSON columns to preserve
variable language keys and other nested structures. Scalar fields retain their
native types. SQL can read the lookup code with `JSON_VALUE(codes, '$.alpha_2')`.

Before merging, validation compares loaded rows with extraction metadata,
checks that returned country codes match the unchecked inputs, and rejects
duplicate or invalid codes. The merge stores the API fields, marks countries
checked, and sets `updated_at` from `data_interval_end` as a DATE. Registered
rows retain their timestamps until a response is merged. Checked countries
are not fetched again unless explicitly reset to unchecked.

Table creation does not migrate existing schemas. HTTP errors fail extraction;
partial responses are not checkpointed across task retries.
