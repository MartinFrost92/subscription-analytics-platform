"""Configuration-driven raw REST extraction and one metadata file per run."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from copy import copy
import json
import math
import os
from itertools import islice
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import threading
import time
from urllib.parse import quote, urljoin, urlsplit
from xml.etree import ElementTree

import requests
import yaml
from airflow.sdk import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def json_path(value, path):
    """Read a dotted JSON path; an omitted path selects the whole response."""
    for part in path.split('.') if path else ():
        value = value[int(part)] if isinstance(value, list) else value[part]
    return value

def normalize_field_names(value):
    """Replace special characters in record keys without losing colliding fields."""
    if isinstance(value, list):
        return [normalize_field_names(item) for item in value]
    if isinstance(value, dict):
        result = {}
        for name, item in value.items():
            normalized = re.sub(r'[^A-Za-z0-9_]', '_', name)
            if normalized in result:
                raise ValueError(f'Field name collision after normalization: {name!r} -> {normalized!r}')
            result[normalized] = normalize_field_names(item)
        return result
    return value


def xml_value(element):
    """Convert XML records to dictionaries, preserving attributes and repeats."""
    value = {'@' + name.rsplit('}', 1)[-1]: text for name, text in element.attrib.items()}
    for child in element:
        name = child.tag.rsplit('}', 1)[-1]
        item = xml_value(child)
        if name not in value:
            value[name] = item
        elif isinstance(value[name], list):
            value[name].append(item)
        else:
            value[name] = [value[name], item]
    text = (element.text or '').strip()
    if not value:
        return text or None
    if text:
        value['#text'] = text
    return value

class _RateLimiter:
    def __init__(self, requests_per_second):
        if not math.isfinite(requests_per_second) or requests_per_second <= 0:
            raise ValueError('requests_per_second must be finite and positive')
        self.spacing = 1 / requests_per_second
        self.next_request = 0.0
        self.lock = threading.Lock()

    def wait(self):
        while True:
            with self.lock:
                delay = self.next_request - time.monotonic()
                if delay <= 0:
                    self.next_request = time.monotonic() + self.spacing
                    return
            time.sleep(delay)

class RestAPIToGCSOperator(BaseOperator):
    """Write raw JSONL and push API-reported totals under extraction_metadata.

    The return value remains a list of GCS URIs for existing load tasks.
    Authentication comes from HttpHook and the configured Airflow connection.
    """

    template_fields = ('config_path', 'gcs_bucket', 'gcs_prefix')

    def __init__(self, *, connection_id, config_path, gcs_bucket, gcs_prefix,
                 endpoint_name=None, local_output_dir=None,
                 max_workers=10, requests_per_second=20,
                 gcp_conn_id='google_cloud_default', connect_timeout=10,
                 read_timeout=60, approved_hosts=(), max_pages=100_000, **kwargs):
        super().__init__(**kwargs)
        if max_workers < 1 or max_pages < 1:
            raise ValueError('max_workers and max_pages must be positive')
        self.connection_id = connection_id
        self.config_path = config_path
        self.endpoint_name = endpoint_name
        self.local_output_dir = local_output_dir
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.max_workers = max_workers
        self.requests_per_second = requests_per_second
        self.gcp_conn_id = gcp_conn_id
        self.timeout = (connect_timeout, read_timeout)
        self.approved_hosts = approved_hosts
        self.max_pages = max_pages

    def _checked_url(self, url):
        parsed = urlsplit(url)
        if (parsed.scheme not in ('http', 'https') or parsed.username or parsed.password
                or (parsed.scheme.lower(), parsed.netloc.lower()) not in self._origins):
            raise ValueError('API URL points to an unapproved origin')
        return url

    def _request(self, method, url, params, response_format='json'):
        if response_format not in ('json', 'xml'):
            raise ValueError('response_format must be json or xml')
        # Keep connection authentication, but give each worker its own session.
        if not hasattr(self._local, 'session'):
            session = requests.Session()
            for attribute in ('headers', 'auth', 'cookies', 'params', 'proxies',
                              'verify', 'cert', 'trust_env', 'stream', 'max_redirects'):
                setattr(session, attribute, copy(getattr(self._http_session, attribute)))
            self._local.session = session
            self._sessions.append(session)
        self._limiter.wait()
        with self._local.session.request(
            method, self._checked_url(url), params=params,
            timeout=self.timeout, allow_redirects=False,
        ) as response:
            response.raise_for_status()
            if 300 <= response.status_code < 400:
                raise requests.HTTPError('API redirect refused', response=response)
            if response_format == 'xml':
                root = ElementTree.fromstring(response.content)
                return {root.tag.rsplit('}', 1)[-1]: xml_value(root)}, response.headers
            return response.json(), response.headers

    def _pages(self, endpoint):
        pagination = endpoint.get('pagination') or {}
        strategy = pagination.get('strategy')
        if strategy not in (None, 'page', 'offset', 'cursor', 'url'):
            raise ValueError(f'Unknown pagination strategy: {strategy}')
        params = {**endpoint.get('params', {}), **pagination.get('params', {})}
        identifier = pagination.get('identifier')
        if strategy in ('page', 'offset'):
            params[identifier] = pagination.get('start', 1 if strategy == 'page' else 0)
        url = urljoin(self._base_url, endpoint['path'])
        seen = set()
        for _ in range(self.max_pages):
            state = (url, json.dumps(params, sort_keys=True))
            if state in seen:
                raise ValueError('Pagination repeated a cursor or URL')
            seen.add(state)
            payload, headers = self._request(
                endpoint.get('request', 'GET'), url, params,
                response_format=endpoint.get('response_format', 'json'),
            )
            records = json_path(payload, endpoint.get('records_path'))
            if endpoint.get('response_format') == 'xml' and not isinstance(records, list):
                records = [] if records is None else [records]
            if not isinstance(records, list):
                raise ValueError('Collection must be a JSON array; configure records_path')
            expected = None
            if endpoint.get('expected_rows_header'):
                expected = headers.get(endpoint['expected_rows_header'])
            elif endpoint.get('expected_rows_path'):
                expected = json_path(payload, endpoint['expected_rows_path'])
            yield records, int(expected) if expected is not None else None
            if strategy is None:
                return
            if strategy in ('page', 'offset'):
                more_path = pagination.get('has_more_path')
                if more_path:
                    more = json_path(payload, more_path)
                    if type(more) is not bool:
                        raise ValueError('has_more_path must select a boolean')
                    if not more:
                        return
                elif not records:
                    return
                params[identifier] += 1 if strategy == 'page' else pagination['step']
            else:
                path = pagination['cursor_path' if strategy == 'cursor' else 'next_url_path']
                try:
                    next_value = json_path(payload, path)
                except (KeyError, IndexError):
                    next_value = None
                if next_value is None:
                    return
                if strategy == 'cursor':
                    params[identifier] = next_value
                else:
                    url, params = urljoin(url, next_value), {}
        raise ValueError('Pagination exceeded max_pages')

    def _detail(self, endpoint, item):
        identifier = json_path(item, endpoint.get('id_field'))
        path = endpoint['detail_path'].replace('{id}', quote(str(identifier), safe=''))
        record, _ = self._request(endpoint.get('request', 'GET'),
                                  urljoin(self._base_url, path), endpoint.get('detail_params'),
                                  response_format=endpoint.get('response_format', 'json'))
        if not isinstance(record, dict):
            raise ValueError('Detail response must be a JSON object')
        return record

    def _extract(self, endpoint, destination, executor):
        metadata = {'name': endpoint['name'], 'expected_rows': None}
        with destination.open('w', encoding='utf-8') as output:
            for page_number, (records, expected) in enumerate(self._pages(endpoint)):
                if page_number == 0:
                    metadata['expected_rows'] = expected
                for start in range(0, len(records), self.max_workers):
                    batch = records[start:start + self.max_workers]
                    results = (executor.map(lambda item: self._detail(endpoint, item), batch)
                               if endpoint.get('detail_path') else batch)
                    for record in results:
                        record = normalize_field_names(record)
                        output.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + '\n')
        return metadata

    def _input_rows(self, endpoint, context, config):
        """Render DAG-relative SQL and stream its BigQuery result rows."""
        root = Path(self.dag.folder) if self.has_dag() else Path(self.config_path).resolve().parent.parent
        sql = (root / endpoint['input_sql']).read_text(encoding='utf-8')
        sql = self.render_template(sql, context)
        location = os.environ.get('BIGQUERY_LOCATION', 'EU')
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, location=location, use_legacy_sql=False)
        client = hook.get_client(project_id=config['project_id'], location=location)
        return client.query(sql, location=location).result()

    def _input_request(self, endpoint, row):
        params = dict(endpoint.get('params', {}))
        for parameter, column in endpoint['request_params'].items():
            if row[column] is None:
                raise ValueError(f'SQL request parameter {parameter!r} cannot be NULL')
            params[parameter] = row[column]
        payload, _ = self._request(
            endpoint.get('request', 'GET'), urljoin(self._base_url, endpoint['path']), params,
            response_format=endpoint.get('response_format', 'json'),
        )
        record = json_path(payload, endpoint.get('records_path'))
        if not isinstance(record, dict):
            raise ValueError('SQL-driven response must select a JSON object; configure records_path')
        return record

    def _extract_sql(self, endpoint, destination, executor, context, config):
        if not isinstance(endpoint.get('request_params'), dict) or not endpoint['request_params']:
            raise ValueError('input_sql requires a nonempty request_params mapping')
        if any(endpoint.get(key) for key in (
                'pagination', 'detail_path', 'expected_rows_header', 'expected_rows_path')):
            raise ValueError('input_sql cannot be combined with pagination, detail requests, or API totals')
        rows = iter(self._input_rows(endpoint, context, config))
        count = 0
        with destination.open('w', encoding='utf-8') as output:
            while batch := list(islice(rows, self.max_workers)):
                # Validate the whole batch before starting its requests.
                for row in batch:
                    for column in endpoint['request_params'].values():
                        if row[column] is None:
                            raise ValueError(f'SQL input column {column!r} cannot be NULL')
                for record in executor.map(lambda row: self._input_request(endpoint, row), batch):
                    output.write(json.dumps(normalize_field_names(record),
                                            ensure_ascii=False, allow_nan=False) + '\n')
                    count += 1
        return {'name': endpoint['name'], 'expected_rows': count}

    def execute(self, context):
        config = yaml.safe_load(Path(self.config_path).read_text(encoding='utf-8'))
        endpoints = [
            {'response_format': config.get('response_format', 'json'), **endpoint}
            for endpoint in config['end_points']
        ]
        if self.endpoint_name is not None:
            endpoints = [item for item in endpoints if item['name'] == self.endpoint_name]
            if len(endpoints) != 1:
                raise ValueError(f'Expected one endpoint named {self.endpoint_name}')
        names = [endpoint['name'] for endpoint in endpoints]
        if len(set(names)) != len(names) or any(not re.fullmatch(r'[A-Za-z0-9_-]+', name) for name in names):
            raise ValueError('Endpoint names must be unique and contain letters, digits, _ or -')
        hook = HttpHook(http_conn_id=self.connection_id)
        self._http_session = hook.get_conn()
        # HttpHook treats unrecognized connection extras as headers. Reserve
        # params for secret query parameters before any request is prepared.
        connection_params = self._http_session.headers.pop('params', {})
        if not isinstance(connection_params, dict):
            self._http_session.close()
            raise ValueError('HTTP connection extra.params must be an object')
        self._http_session.params.update(connection_params)
        self._sessions = [self._http_session]
        self._local = threading.local()
        self._local.session = self._http_session
        self._base_url = hook.url_from_endpoint('').rstrip('/') + '/'
        self._origins = {(urlsplit(url).scheme.lower(), urlsplit(url).netloc.lower())
                         for url in (self._base_url, *self.approved_hosts)}
        self._limiter = _RateLimiter(self.requests_per_second)
        interval = f"{context['data_interval_start'].isoformat()}_{context['data_interval_end'].isoformat()}"
        if self.local_output_dir:
            run_folder = re.sub(r'[^A-Za-z0-9_.-]', '_', context.get('run_id') or interval)
            directory = Path(self.local_output_dir) / run_folder / self.task_id
            directory.mkdir(parents=True, exist_ok=True)
            staging = nullcontext(directory)
        else:
            staging = TemporaryDirectory()
        metadata, paths = [], []
        try:
            with staging as directory, ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                gcs = GCSHook(gcp_conn_id=self.gcp_conn_id)
                for endpoint in endpoints:
                    path = Path(directory) / (endpoint['name'] + '.jsonl')
                    if endpoint.get('input_sql'):
                        metadata.append(self._extract_sql(endpoint, path, executor, context, config))
                    else:
                        metadata.append(self._extract(endpoint, path, executor))
                    object_name = '/'.join(filter(None, (self.gcs_prefix.strip('/'), endpoint['name'], interval + '.jsonl')))
                    gcs.upload(bucket_name=self.gcs_bucket, object_name=object_name,
                               filename=str(path), mime_type='application/x-ndjson')
                    paths.append(f'gs://{self.gcs_bucket}/{object_name}')
            context['ti'].xcom_push(key='extraction_metadata', value=metadata)
            return paths
        finally:
            for session in self._sessions:
                session.close()


class ConsolidateExtractionMetadataOperator(BaseOperator):
    """Write all successful endpoint metadata once, after extraction tasks finish."""

    template_fields = ('gcs_bucket',)

    def __init__(self, *, extraction_task_ids, gcs_bucket,
                 gcp_conn_id='google_cloud_default', **kwargs):
        super().__init__(**kwargs)
        self.extraction_task_ids = extraction_task_ids
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        rows = []
        for task_id in self.extraction_task_ids:
            rows.extend(context['ti'].xcom_pull(task_ids=task_id, key='extraction_metadata'))
        timestamp = context['data_interval_end'].strftime('%Y%m%dT%H%M%S')
        object_name = f"utilities/{timestamp}_{context['dag'].dag_id}_metadata.json"
        GCSHook(gcp_conn_id=self.gcp_conn_id).upload(
            bucket_name=self.gcs_bucket, object_name=object_name,
            data=''.join(json.dumps(row) + '\n' for row in rows),
            mime_type='application/x-ndjson',
        )
        return f'gs://{self.gcs_bucket}/{object_name}'
