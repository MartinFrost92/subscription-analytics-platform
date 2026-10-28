"""Model wiring tests, plus opt-in BigQuery SQL fixture checks.

Run BigQuery fixtures with RUN_BIGQUERY_FIXTURES=1 inside the Airflow container.
The fixtures use inline data and session-local temporary tables only.
"""
import json
import os
from pathlib import Path
import runpy
import unittest

import pendulum


MODEL_DIR = Path(__file__).resolve().parents[2] / 'dags/model/synthetic_api'


class ModelTests(unittest.TestCase):
    def setUp(self):
        self.dag = runpy.run_path(str(MODEL_DIR / 'subscriptions.py'))['dag']
        self.context = dict(dag=self.dag, params=self.dag.params,
                            data_interval_end=pendulum.datetime(2026, 10, 28))

    def test_dimensions_before_fact_work_and_validation_after_loads(self):
        for entry in self.dag.params['delta_load_tables']:
            name = entry['name']
            work = self.dag.get_task('tmp_' + name)
            self.assertEqual(work.upstream_task_ids,
                             {'create_subscriptions_tables'} |
                             {'full_load_' + dimension for dimension in entry['depends_on']})
            self.assertEqual(self.dag.get_task('delta_load_' + name).upstream_task_ids,
                             {work.task_id})
        self.assertEqual(self.dag.leaves[0].task_id, 'validate_data')

    def test_table_resources_and_all_templates(self):
        for task in self.dag.tasks:
            task.render_template_fields(self.context)
        tables = json.loads(self.dag.get_task('create_subscriptions_tables').tables)
        self.assertEqual(len(tables), 7)
        for table in tables:
            self.assertEqual(table['dataset_id'], 'model')
            self.assertTrue(all('name' in field and 'type' in field
                                for field in table['schema_fields']))
            names = [field['name'] for field in table['schema_fields']]
            self.assertEqual(len(names), len(set(names)))
            if table['table_id'].startswith('fact_'):
                self.assertNotIn('charge', names)
                self.assertNotIn('discount', names)
                self.assertEqual(table['timePartitioning']['field'], 'updated_at')
                self.assertNotIn('date_key', names)
                if table['table_id'] == 'fact_subscription_inventory':
                    self.assertEqual(next(field['type'] for field in table['schema_fields']
                                          if field['name'] == 'is_sale'), 'BOOLEAN')
            else:
                self.assertNotIn('updated_at', names)
                if table['table_id'] == 'dim_date':
                    self.assertEqual(names[0], 'date')
                if table['table_id'] == 'dim_subscription':
                    self.assertNotIn('product', names)
        for entry in self.dag.params['delta_load_tables']:
            work = self.dag.get_task('tmp_' + entry['name'])
            self.assertEqual(work.configuration['query']['createDisposition'], 'CREATE_IF_NEEDED')
            self.assertEqual(work.configuration['query']['writeDisposition'], 'WRITE_TRUNCATE')

    def test_boolean_is_generated_in_transform_and_passed_through(self):
        transform_dir = MODEL_DIR.parents[1] / 'transform/synthetic_api'
        transform_dag = runpy.run_path(str(transform_dir / 'subscriptions.py'))['dag']
        transform_work = transform_dag.get_task('tmp_subscription_inventory')
        transform_work.render_template_fields(dict(
            dag=transform_dag, params=transform_dag.params,
            data_interval_end=self.context['data_interval_end']))
        sql = transform_work.configuration['query']['query']
        self.assertIn('TRUE AS is_sale', sql)
        self.assertIn('FALSE AS is_sale', sql)
        self.assertNotIn('event_type', sql)
        work = self.dag.get_task('tmp_fact_subscription_inventory')
        work.render_template_fields(self.context)
        self.assertNotIn('event_type', work.configuration['query']['query'])
        self.assertNotIn('CASE', work.configuration['query']['query'])

    @unittest.skipUnless(os.environ.get('RUN_BIGQUERY_FIXTURES') == '1', 'opt-in BigQuery fixtures')
    def test_fact_sql_with_inline_bigquery_fixtures(self):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud import bigquery

        fixtures = """
fixture_customers AS (
    SELECT 1 AS account_id, 'Alice' AS name, 'DK' AS country,
           TIMESTAMP '2025-01-01' AS created_at, DATE '2026-10-27' AS updated_at
),
fixture_subscriptions AS (
    SELECT 10 AS subscription_id, 1 AS account_id, 'Basic' AS product,
           DATE '2026-01-01' AS start_date, DATE '2026-10-20' AS stop_date,
           'cancelled' AS status, DATE '2026-10-27' AS updated_at
),
fixture_invoices AS (
    SELECT 100 AS invoice_number, 1 AS account_id, 10 AS subscription_id,
           DATE '2026-01-01' AS start_date, DATE '2026-01-04' AS stop_date,
           NUMERIC '105' AS charge, NUMERIC '5' AS discount, DATE '2026-10-27' AS updated_at
),
fixture_inventory AS (
    SELECT DATE '2026-01-01' AS date, 1 AS account_id, 10 AS subscription_id,
           'Basic' AS product, TRUE AS is_sale, DATE '2026-10-27' AS updated_at
    UNION ALL
    SELECT DATE '2026-10-20', 1, 10, 'Basic', FALSE, DATE '2026-10-27'
),
fixture_revenue AS (
    SELECT revenue_date AS date, 100 AS invoice_number, 1 AS account_id,
           10 AS subscription_id,
           IF(revenue_date = DATE '2026-01-03', NUMERIC '33.333333334', NUMERIC '33.333333333') AS daily_revenue,
           DATE '2026-10-27' AS updated_at
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2026-01-01', DATE '2026-01-03')) AS revenue_date
)
"""
        project = self.dag.params['project_id']
        replacements = {
            f'`{project}.staging.customers`': 'fixture_customers',
            f'`{project}.staging.subscriptions`': 'fixture_subscriptions',
            f'`{project}.staging.invoices`': 'fixture_invoices',
            f'`{project}.transform.subscription_inventory`': 'fixture_inventory',
            f'`{project}.transform.invoice_revenue`': 'fixture_revenue',
        }
        queries = {}
        names = ['dim_customer', 'dim_product', 'dim_subscription', 'dim_invoice',
                 'tmp_fact_revenue', 'tmp_fact_subscription_inventory']
        for name in names:
            sql = self.dag.get_template_env().get_template(f'queries/production/{name}.sql').render(**self.context)
            for original, replacement in replacements.items():
                sql = sql.replace(original, replacement)
            for fact in ['fact_revenue', 'fact_subscription_inventory']:
                sql = sql.replace(f'`{project}.model.{fact}`', 'UNNEST(ARRAY<INT64>[])')
            queries[name] = sql.strip().rstrip(';')

        def query_sql():
            return 'WITH ' + fixtures + '\nSELECT\n' + ',\n'.join(
                f'ARRAY(SELECT AS STRUCT * FROM ({sql})) AS {name}' for name, sql in queries.items())

        client = BigQueryHook(gcp_conn_id='google_cloud_default', location='EU',
                              use_legacy_sql=False).get_client()
        try:
            config = bigquery.QueryJobConfig(maximum_bytes_billed=1024 * 1024)
            row = list(client.query(query_sql(), location='EU', job_config=config).result(timeout=60))[0]
            revenue = row['tmp_fact_revenue']
            self.assertEqual(len(revenue), 3)
            from decimal import Decimal
            self.assertEqual(sum(record['daily_revenue'] for record in revenue), Decimal('100'))
            self.assertEqual(len(row['tmp_fact_subscription_inventory']), 2)
            self.assertEqual({record['is_sale'] for record in row['tmp_fact_subscription_inventory']}, {True, False})
            for fact in ['tmp_fact_revenue', 'tmp_fact_subscription_inventory']:
                for record in row[fact]:
                    for dimension, column in [('dim_customer', 'customer_key'),
                                              ('dim_product', 'product_key'),
                                              ('dim_subscription', 'subscription_key')]:
                        self.assertEqual(record[column], row[dimension][0][column])
            self.assertEqual(revenue[0]['invoice_key'], row['dim_invoice'][0]['invoice_key'])

            # Execute the complete monitoring SQL against the same fixture model.
            model_ctes = []
            for name in ['dim_date', 'dim_customer', 'dim_product', 'dim_subscription',
                         'dim_invoice', 'fact_revenue', 'fact_subscription_inventory']:
                if name == 'dim_date':
                    sql = self.dag.get_template_env().get_template('queries/production/dim_date.sql').render(**self.context).strip().rstrip(';')
                else:
                    sql = queries.get(name, queries.get('tmp_' + name))
                model_ctes.append(f'model_{name} AS ({sql})')
            validation = self.dag.get_template_env().get_template('queries/monitoring/data_validation.sql').render(**self.context)
            for original, replacement in replacements.items():
                validation = validation.replace(original, replacement)
            for name in ['dim_date', 'dim_customer', 'dim_product', 'dim_subscription',
                         'dim_invoice', 'fact_revenue', 'fact_subscription_inventory']:
                validation = validation.replace(f'`{project}.model.{name}`', 'model_' + name)
            validation_query = ('WITH ' + fixtures + ',\n' + ',\n'.join(model_ctes)
                                + '\nSELECT * FROM (' + validation + ')')
            checks = list(client.query(validation_query, location='EU', job_config=config).result(timeout=60))
            self.assertEqual(len(checks), 2)
            self.assertTrue(all(check['diff'] == Decimal('0') for check in checks), checks)

            # Exercise the actual DELETE/INSERT scripts on session-local tables.
            statements = []
            for fixture in ['fixture_subscriptions', 'fixture_customers']:
                statements.append(f'CREATE TEMP TABLE {fixture} AS WITH {fixtures} SELECT * FROM {fixture};')
            for fact in ['fact_revenue', 'fact_subscription_inventory']:
                statements.append(f'CREATE TEMP TABLE work_{fact} AS WITH {fixtures} {queries["tmp_" + fact]};')
                statements.append(f'CREATE TEMP TABLE target_{fact} AS SELECT * FROM work_{fact};')

            def replacement_sql(fact):
                sql = self.dag.get_template_env().get_template(f'queries/production/{fact}.sql').render(**self.context)
                sql = sql.replace(f'`{project}.model.{fact}`', 'target_' + fact)
                sql = sql.replace(f'`{project}.utilities.tmp_{fact}`', 'work_' + fact)
                for original, replacement in replacements.items():
                    sql = sql.replace(original, replacement)
                return sql

            for _ in range(2):
                for fact in ['fact_revenue', 'fact_subscription_inventory']:
                    statements.append(replacement_sql(fact))
                    statements.append(f"ASSERT (SELECT COUNT(*) FROM target_{fact}) = (SELECT COUNT(*) FROM work_{fact}) AS 'Replacement must not duplicate rows';")
            statements.append("DELETE FROM work_fact_revenue WHERE date = DATE '2026-01-03';")
            statements.append(replacement_sql('fact_revenue'))
            statements.append("ASSERT (SELECT COUNT(*) FROM target_fact_revenue) = 2 AS 'Obsolete revenue dates must disappear';")
            statements.append('DELETE FROM work_fact_subscription_inventory WHERE NOT is_sale;')
            statements.append("UPDATE fixture_subscriptions SET stop_date = NULL, updated_at = DATE '2026-10-28' WHERE TRUE;")
            statements.append(replacement_sql('fact_subscription_inventory'))
            statements.append("ASSERT (SELECT COUNT(*) FROM target_fact_subscription_inventory WHERE NOT is_sale) = 0 AS 'Removed cancellations must disappear';")
            # BigQuery bills temporary-table scans in minimum 10 MB increments.
            script_config = bigquery.QueryJobConfig(maximum_bytes_billed=512 * 1024 * 1024)
            list(client.query('\n'.join(statements), location='EU', job_config=script_config).result(timeout=60))

            # An empty target bootstraps old records; a nonempty target respects the cutoff.
            fixtures = fixtures.replace("DATE '2026-10-27'", "DATE '2026-01-01'")
            row = list(client.query(query_sql(), location='EU', job_config=config).result(timeout=60))[0]
            self.assertEqual(len(row['tmp_fact_revenue']), 3)
            queries = {name: sql.replace('UNNEST(ARRAY<INT64>[])', 'UNNEST([1])')
                       for name, sql in queries.items()}
            row = list(client.query(query_sql(), location='EU', job_config=config).result(timeout=60))[0]
            self.assertEqual(len(row['tmp_fact_revenue']), 0)
            self.assertEqual(len(row['tmp_fact_subscription_inventory']), 0)

        finally:
            client.close()
