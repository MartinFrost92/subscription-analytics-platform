# Subscription star schema

Run `model_synthetic_api_subscriptions` after the transform DAG succeeds.
The DAG full-loads current dimensions, prepares changed fact rows in utilities,
replaces the affected fact rows, then validates the finished model.

| Table | Grain | Power BI relationships |
| --- | --- | --- |
| dim_date | One date, using the existing 2025–2030 date SQL | date |
| dim_customer | One account, current attributes | customer_key |
| dim_product | One current product name | product_key |
| dim_subscription | One subscription, current attributes | subscription_key |
| dim_invoice | One invoice, including its period, charge and discount | invoice_key |
| fact_revenue | One invoice per revenue date | All five dimensions |
| fact_subscription_inventory | One subscription start or cancellation | Date, Customer, Product, Subscription |

Use one-to-many, single-direction relationships from dimensions to facts.
Dimensions connect directly to facts; no dimension-to-dimension relationships
are required. Transform generates `is_sale`: TRUE for starts and FALSE for
cancellations. The fact stores that boolean unchanged. Count TRUE rows for
sales and FALSE rows for churn; inventory is cumulative sales minus churn.
`charge` and `discount` are invoice attributes, not daily fact measures.

Customer, subscription, and invoice keys use their existing numeric IDs, aliased
as `customer_key`, `subscription_key`, and `invoice_key`. Only Product uses
`FARM_FINGERPRINT(CAST(product AS STRING))`, consistently in its dimension and
facts. Date joins directly on `date`. Revenue uniqueness is `(invoice_key, date)`;
subscription events are unique by `(subscription_key, is_sale)`. No extra fact row keys
are generated. Dimensions omit audit dates, and product attributes live in
`dim_product` rather than being repeated in `dim_subscription`.

`variables.date_window` controls the lookback from `data_interval_end` (seven
days by default). Empty fact targets bootstrap all history. Work tables are
created automatically in utilities and are not in `tables.table.json`.
They are named `tmp_fact_revenue` and `tmp_fact_subscription_inventory`.
Fact replacements use separate DELETE and INSERT statements, without an explicit
transaction, matching the transform pattern. Retry the delta task if insertion
fails after deletion; its work table remains available. Avoid overlapping runs
that share the same work-table names.

All events for an affected subscription are replaced together. Its Sale row
keeps the subscription in the work table even if its Cancellation disappears,
so no separate churn deletion lookup is required. Source deletions and missed updates older than the lookback require
a wider lookback or a deliberate reload.

Facts partition by `updated_at` and cluster by customer/subscription keys.
Power BI incremental refresh policy is configured separately; historical event
changes still require refreshing their date partitions. Dates outside the
template's date range fail dimension-reference validation; extend the date
dimension range when needed.

Monitoring checks revenue totals and subscription inventory row counts against
transform, with zero tolerance.
