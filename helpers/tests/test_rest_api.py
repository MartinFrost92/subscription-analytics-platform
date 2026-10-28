"""Focused REST extraction tests; no real API or Google Cloud calls."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import runpy
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import pendulum
import requests
import yaml
from xml.etree.ElementTree import ParseError
from airflow.sdk import Connection
from utilities.rest_api import (
    ConsolidateExtractionMetadataOperator, RestAPIToGCSOperator, _RateLimiter,
)


class RestTests(unittest.TestCase):
    def operator(self, **kwargs):
        operator = RestAPIToGCSOperator(
            task_id='group.extract_customers', connection_id='api', config_path='unused',
            gcs_bucket='raw', gcs_prefix='staging/', **kwargs,
        )
        operator._base_url = 'https://api.example/'
        return operator

    def pages(self, endpoint, responses):
        operator = self.operator()
        with patch.object(operator, '_request', side_effect=responses) as request:
            pages = list(operator._pages(endpoint))
            calls = request.call_args_list
        return pages, calls

    def test_page_and_offset(self):
        for strategy, start, step in [('page', 1, 1), ('offset', 0, 2)]:
            with self.subTest(strategy=strategy):
                # Snapshot params because the pagination iterator updates its dictionary.
                operator = self.operator()
                requested = []
                responses = iter([([1, 2], {}), ([], {})])
                def request(method, url, params, **kwargs):
                    requested.append(dict(params))
                    return next(responses)
                endpoint = dict(name='customers', path='customers', pagination=dict(
                    strategy=strategy, identifier='position', step=step, params={'size': 2}))
                with patch.object(operator, '_request', side_effect=request):
                    self.assertEqual(list(operator._pages(endpoint)), [([1, 2], None), ([], None)])
                self.assertEqual(requested, [{'size': 2, 'position': start},
                                             {'size': 2, 'position': start + step}])

    def test_cursor_and_url(self):
        for strategy, path, next_value in [('cursor', 'cursor_path', 'abc'),
                                           ('url', 'next_url_path', '/next')]:
            with self.subTest(strategy=strategy):
                endpoint = dict(name='customers', path='customers', records_path='items',
                                expected_rows_path='total', pagination=dict(
                                    strategy=strategy, identifier='cursor', **{path: 'next'}))
                pages, calls = self.pages(endpoint, [
                    ({'items': [1], 'total': 42, 'next': next_value}, {}),
                    ({'items': [2], 'total': 42, 'next': None}, {})])
                self.assertEqual(pages, [([1], 42), ([2], 42)])
                if strategy == 'url':
                    self.assertEqual(calls[1].args[1], 'https://api.example/next')
                else:
                    self.assertEqual(calls[1].args[2]['cursor'], 'abc')

    def test_empty_page_with_continuation_is_not_reconciliation_failure(self):
        endpoint = dict(name='customers', path='customers', records_path='items',
                        pagination=dict(strategy='page', identifier='page', has_more_path='more'))
        pages, _ = self.pages(endpoint, [({'items': [], 'more': True}, {}),
                                        ({'items': [1], 'more': False}, {})])
        self.assertEqual(pages, [([], None), ([1], None)])

    def test_pagination_loop_guard(self):
        endpoint = dict(name='customers', path='customers', records_path='items',
                        pagination=dict(strategy='cursor', identifier='cursor', cursor_path='next'))
        with self.assertRaisesRegex(ValueError, 'repeated'):
            self.pages(endpoint, [({'items': [1], 'next': 'same'}, {})] * 3)

    def test_bad_collection_propagates(self):
        with self.assertRaisesRegex(ValueError, 'JSON array'):
            self.pages(dict(name='customers', path='customers'), [({'unexpected': 1}, {})])

    def execute_extraction(self, endpoints, responses, config_response_format=None, **kwargs):
        uploads = {}
        response_iterator = iter(responses)
        observed = []
        def request(session, method, url, **options):
            observed.append((session.auth, session.headers.get('X-API-Key'), url))
            payload, headers, status = next(response_iterator)
            response = requests.Response()
            response.status_code = status
            response._content = (payload.encode('utf-8') if isinstance(payload, str)
                                 else json.dumps(payload).encode('utf-8'))
            response._content_consumed = True
            response.headers.update(headers)
            return response
        def upload(**options):
            uploads[options['object_name']] = Path(options['filename']).read_text(encoding='utf-8')
        connection = Connection(conn_id='api', conn_type='http', host='https://api.example',
                                login='user', password='password', extra='{"X-API-Key":"secret"}')
        ti = MagicMock()
        with TemporaryDirectory() as directory:
            config_path = Path(directory, 'config.yml')
            config = {'end_points': endpoints}
            if config_response_format is not None:
                config['response_format'] = config_response_format
            config_path.write_text(yaml.safe_dump(config), encoding='utf-8')
            operator = self.operator(**kwargs)
            operator.config_path = str(config_path)
            context = dict(ti=ti, run_id='manual__test',
                           data_interval_start=pendulum.datetime(2026, 10, 28),
                           data_interval_end=pendulum.datetime(2026, 10, 29))
            with patch('utilities.rest_api.HttpHook.get_connection', return_value=connection), \
                    patch.object(requests.Session, 'request', autospec=True, side_effect=request), \
                    patch('utilities.rest_api.GCSHook') as gcs:
                gcs.return_value.upload.side_effect = upload
                result = operator.execute(context)
        return uploads, ti, result, observed

    def test_xml_records_attributes_namespaces_and_total(self):
        endpoint = dict(name='rates', path='rates', response_format='xml',
                        records_path='rates.rate', expected_rows_path='rates.@total')
        xml = ('<rates xmlns="urn:rates" total="20">'
               '<rate code="EUR"><value>7.46</value></rate>'
               '<rate code="USD"><value>6.85</value></rate></rates>')
        uploads, ti, _, _ = self.execute_extraction([endpoint], [(xml, {}, 200)])
        self.assertEqual([json.loads(line) for line in next(iter(uploads.values())).splitlines()],
                         [{'@code': 'EUR', 'value': '7.46'}, {'@code': 'USD', 'value': '6.85'}])
        ti.xcom_push.assert_called_once_with(key='extraction_metadata',
                                           value=[{'name': 'rates', 'expected_rows': 20}])

    def test_xml_singleton_and_empty_record_selection(self):
        for xml, path, expected in [
            ('<rates><rate code="EUR">7.46</rate></rates>', 'rates.rate',
             [{'@code': 'EUR', '#text': '7.46'}]),
            ('<rates/>', 'rates', []),
        ]:
            with self.subTest(xml=xml):
                endpoint = dict(name='rates', path='rates', response_format='xml', records_path=path)
                uploads, _, _, _ = self.execute_extraction([endpoint], [(xml, {}, 200)])
                self.assertEqual([json.loads(line) for line in next(iter(uploads.values())).splitlines()], expected)

    def test_xml_config_default_and_json_endpoint_override(self):
        endpoints = [dict(name='rates', path='rates', records_path='rates.rate'),
                     dict(name='customers', path='customers', response_format='json')]
        uploads, _, _, _ = self.execute_extraction(
            endpoints, [('<rates><rate code="EUR"/></rates>', {}, 200), ([{'id': 1}], {}, 200)],
            config_response_format='xml')
        records = [[json.loads(line) for line in text.splitlines()] for text in uploads.values()]
        self.assertEqual(records, [[{'@code': 'EUR'}], [{'id': 1}]])

    def test_xml_pagination_and_detail_requests(self):
        endpoint = dict(name='items', path='items', response_format='xml',
                        records_path='response.item', id_field='@id', detail_path='items/{id}',
                        pagination=dict(strategy='cursor', identifier='cursor', cursor_path='response.next'))
        responses = [('<response><item id="1"/><next>second</next></response>', {}, 200),
                     ('<item id="1"><name>first</name></item>', {}, 200),
                     ('<response><item id="2"/><next/></response>', {}, 200),
                     ('<item id="2"><name>second</name></item>', {}, 200)]
        uploads, _, _, _ = self.execute_extraction([endpoint], responses)
        self.assertEqual([json.loads(line) for line in next(iter(uploads.values())).splitlines()],
                         [{'item': {'@id': '1', 'name': 'first'}},
                          {'item': {'@id': '2', 'name': 'second'}}])

    def test_invalid_xml_and_response_format_fail(self):
        with self.assertRaises(ParseError):
            self.execute_extraction([dict(name='rates', path='rates', response_format='xml')],
                                    [('<rates>', {}, 200)])
        with self.assertRaisesRegex(ValueError, 'response_format'):
            self.execute_extraction([dict(name='rates', path='rates', response_format='csv')], [])

    def test_auth_raw_json_and_independent_totals(self):
        endpoints = [dict(name='customers', path='customers', detail_path='customers/{id}',
                          expected_rows_header='X-Total-Count')]
        uploads, ti, paths, observed = self.execute_extraction(endpoints, [
            ([7, 7], {'X-Total-Count': '3000'}, 200),
            ({'id': 7, 'raw': 'æ', 'nullable': None}, {}, 200),
            ({'id': 7, 'raw': 'æ', 'nullable': None}, {}, 200)])
        records = [json.loads(line) for line in next(iter(uploads.values())).splitlines()]
        self.assertEqual(records, [{'id': 7, 'raw': 'æ', 'nullable': None}] * 2)
        ti.xcom_push.assert_called_once_with(key='extraction_metadata',
                                           value=[{'name': 'customers', 'expected_rows': 3000}])
        self.assertTrue(paths[0].startswith('gs://raw/staging/customers/'))
        self.assertTrue(all(auth.username == 'user' and auth.password == 'password' and key == 'secret'
                            for auth, key, _ in observed))

    def test_unavailable_total_and_multiple_endpoints(self):
        endpoints = [dict(name=name, path=name) for name in ['customers', 'subscriptions']]
        uploads, ti, paths, _ = self.execute_extraction(endpoints, [([], {}, 200), ([], {}, 200)])
        self.assertEqual(len(uploads), 2)
        self.assertEqual(len(paths), 2)
        ti.xcom_push.assert_called_once_with(key='extraction_metadata', value=[
            {'name': 'customers', 'expected_rows': None},
            {'name': 'subscriptions', 'expected_rows': None}])

    def test_http_failure_propagates_without_retry(self):
        with self.assertRaises(requests.HTTPError):
            self.execute_extraction([dict(name='customers', path='customers')],
                                    [({'error': 'unavailable'}, {}, 503)])

    def test_invalid_json_propagates(self):
        with patch.object(requests.Response, 'json', side_effect=ValueError('invalid JSON')):
            with self.assertRaisesRegex(ValueError, 'invalid JSON'):
                self.execute_extraction([dict(name='customers', path='customers')],
                                        [([], {}, 200)])

    def test_origin_and_redirect_guards(self):
        operator = self.operator()
        operator._origins = {('https', 'api.example')}
        with self.assertRaisesRegex(ValueError, 'unapproved'):
            operator._checked_url('https://other.example/records')
        with self.assertRaisesRegex(requests.HTTPError, 'redirect refused'):
            self.execute_extraction([dict(name='customers', path='customers')],
                                    [([], {'Location': 'https://other.example'}, 302)])

    def test_local_output_is_overwritten(self):
        operator = self.operator()
        with TemporaryDirectory() as directory, ThreadPoolExecutor(max_workers=1) as executor:
            path = Path(directory, 'customers.jsonl')
            endpoint = dict(name='customers')
            with patch.object(operator, '_pages', return_value=iter([([{'id': 1}], None)])):
                operator._extract(endpoint, path, executor)
            with patch.object(operator, '_pages', return_value=iter([([{'id': 2}], None)])):
                operator._extract(endpoint, path, executor)
            self.assertEqual(path.read_text(), '{"id": 2}\n')

    def test_rate_limiter(self):
        limiter = _RateLimiter(20)
        with patch('utilities.rest_api.time.monotonic', side_effect=[10, 10, 10, 10.05, 10.05]), \
                patch('utilities.rest_api.time.sleep') as sleep:
            limiter.wait()
            limiter.wait()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.05)

    def test_consolidation_and_stable_overwrite_key(self):
        operator = ConsolidateExtractionMetadataOperator(
            task_id='metadata', extraction_task_ids=['extract_customers', 'extract_invoices'],
            gcs_bucket='raw')
        rows = [[{'name': 'customers', 'expected_rows': None}],
                [{'name': 'invoices', 'expected_rows': 3000}]]
        ti = MagicMock()
        ti.xcom_pull.side_effect = rows * 2
        context = dict(ti=ti, dag=SimpleNamespace(dag_id='subscription_staging'),
                       data_interval_end=pendulum.datetime(2026, 10, 29))
        with patch('utilities.rest_api.GCSHook') as gcs:
            first = operator.execute(context)
            second = operator.execute(context)
        self.assertEqual(first, second)
        self.assertEqual(first, 'gs://raw/utilities/20261029T000000_subscription_staging_metadata.json')
        calls = gcs.return_value.upload.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].kwargs, calls[1].kwargs)
        self.assertEqual([json.loads(line) for line in calls[0].kwargs['data'].splitlines()],
                         rows[0] + rows[1])

    def test_dag_dependencies_and_rendered_destinations(self):
        dag_path = Path(__file__).resolve().parents[2] / 'dags/staging/synthetic_subscription_api/import.py'
        dag = runpy.run_path(str(dag_path))['dag']
        context = dict(dag=dag, params=dag.params,
                       data_interval_end=pendulum.datetime(2026, 10, 29))
        create = dag.get_task('create_staging_tables')
        create.render_template_fields(context)
        tables = json.loads(create.tables)
        metadata = dag.get_task('consolidate_extraction_metadata')
        self.assertEqual(metadata.upstream_task_ids,
                         {'extract_customers', 'extract_subscriptions', 'extract_invoices'})
        self.assertEqual(metadata.trigger_rule, 'all_success')
        for name, table in zip(['customers', 'subscriptions', 'invoices'], tables):
            load = dag.get_task(f'load_{name}')
            # Render just the destination; source_objects requires a live TaskInstance.
            destination = load.render_template(load.destination_project_dataset_table, context)
            self.assertEqual(destination,
                             f"{dag.params['project_id']}.{table['dataset_id']}.{table['table_id']}")
            self.assertEqual(table['table_id'], f'20261029T000000_{dag.dag_id}_{name}')
            self.assertEqual(load.upstream_task_ids, {f'extract_{name}'})


if __name__ == '__main__':
    unittest.main()
