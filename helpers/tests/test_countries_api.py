"""Countries DAG wiring and extraction checks without live API or cloud calls."""
import json
from pathlib import Path
import runpy
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pendulum
import requests

from utilities.rest_api import RestAPIToGCSOperator


class CountriesTests(TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parents[2] / 'dags/staging/countries_api'
        self.dag = runpy.run_path(str(self.root / 'import.py'))['dag']
        self.context = dict(
            dag=self.dag, params=self.dag.params,
            ti=SimpleNamespace(
                xcom_pull=lambda task_ids: ['gs://raw/staging/countries/test.jsonl']
                if task_ids == 'extract_countries' else 'gs://raw/utilities/metadata.json',
                xcom_push=MagicMock(),
            ),
            run_id='countries_test',
            data_interval_start=pendulum.datetime(2026, 10, 28, 5),
            data_interval_end=pendulum.datetime(2026, 10, 29, 5),
        )

    def test_dependencies_and_rendered_table_definitions(self):
        expected = {
            'create_staging_tables': {'wait_for_staging_synthetic_subscription_api_import'},
            'register_countries': {'create_staging_tables'},
            'extract_countries': {'register_countries'},
            'load_countries': {'extract_countries'},
            'validate_data': {'load_countries', 'load_metadata'},
            'delta_load_countries': {'validate_data'},
        }
        for task_id, upstream in expected.items():
            self.assertEqual(self.dag.get_task(task_id).upstream_task_ids, upstream)
        for task in self.dag.tasks:
            task.render_template_fields(self.context)
        tables = json.loads(self.dag.get_task('create_staging_tables').tables)
        load = self.dag.get_task('load_countries')
        self.assertEqual(load.destination_project_dataset_table,
                         'healthy-pager-398915.utilities.' + tables[0]['table_id'])
        self.assertEqual(tables[1]['table_id'], 'countries')
        self.assertEqual(tables[1]['clustering']['fields'], ['country_id'])
        fields = {f['name']: f for f in tables[1]['schema_fields']}
        self.assertEqual(fields['updated_at']['type'], 'DATE')
        self.assertEqual(fields['is_checked']['type'], 'BOOLEAN')
        self.assertEqual(fields['names']['type'], 'JSON')
        register = self.dag.get_task('register_countries').configuration['query']['query']
        self.assertIn("FALSE, DATE('2026-10-29')", register)
        self.assertNotIn('WHEN MATCHED', register)

    def test_extracts_country_object_and_preserves_dynamic_nested_data(self):
        record = {
            'codes': {'alpha_2': 'DK'},
            'names': {'common': 'Denmark', 'native': {'dan': {'common': 'Danmark'}}},
            'currencies': [{'code': 'DKK', 'name': 'Danish krone'}],
            'population': 6038638,
        }
        uploads = []
        task = self.dag.get_task('extract_countries')
        task.render_template_fields(self.context)
        with TemporaryDirectory() as directory:
            task.local_output_dir = directory
            with patch('utilities.rest_api.HttpHook') as hook, \
                    patch('utilities.rest_api.GCSHook') as gcs, \
                    patch.object(RestAPIToGCSOperator, '_input_rows',
                                 return_value=iter([{'country_id': 'DK'}])), \
                    patch.object(RestAPIToGCSOperator, '_request',
                                 return_value=({'data': {'objects': [record]}}, {})) as request:
                hook.return_value.get_conn.return_value = requests.Session()
                hook.return_value.url_from_endpoint.return_value = 'https://api.restcountries.com/'
                gcs.return_value.upload.side_effect = lambda **kw: uploads.append(
                    Path(kw['filename']).read_text(encoding='utf-8'))
                task.execute(self.context)
            self.assertEqual(json.loads(uploads[0]), record)
            self.assertEqual(request.call_args.args[1],
                             'https://api.restcountries.com/countries/v5/code')
            self.assertEqual(request.call_args.args[2], {'q': 'DK'})
            self.context['ti'].xcom_push.assert_called_once_with(
                key='extraction_metadata', value=[{'name': 'countries', 'expected_rows': 1}])
        schema = json.loads((self.root / 'schemas/tmp_countries.schema.json').read_text())
        fields = {f['name']: f for f in schema}
        self.assertLessEqual(record.keys(), fields.keys())
        for name in ['codes', 'names', 'currencies']:
            self.assertEqual(fields[name]['type'], 'JSON')
