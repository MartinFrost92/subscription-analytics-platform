"""Configuration-driven JSON REST extraction with staged JSONL uploads."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from copy import copy
from email.utils import parsedate_to_datetime
import json
import math
from pathlib import Path
import random
import re
from tempfile import TemporaryDirectory
import threading
import time
from urllib.parse import quote, urljoin, urlsplit

import requests
import yaml
from airflow.sdk import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def json_path(value, path, *, optional=False):
    if not path:
        return value
    try:
        for part in path.split('.'):
            value = value[int(part)] if isinstance(value, list) else value[part]
        return value
    except (KeyError, IndexError):
        if optional:
            return None
        raise


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
                now = time.monotonic()
                delay = self.next_request - now
                if delay <= 0:
                    self.next_request = now + self.spacing
                    return
            time.sleep(delay)

    def cooldown(self, seconds):
        with self.lock:
            self.next_request = max(self.next_request, time.monotonic() + seconds)


class RestAPIToGCSOperator(BaseOperator):
    template_fields = ('config_path', 'gcs_bucket', 'gcs_prefix')

    def __init__(self, *, connection_id, config_path, gcs_bucket, gcs_prefix,
                 endpoint_name=None, local_output_dir=None,
                 max_workers=10, requests_per_second=20,
                 gcp_conn_id='google_cloud_default', connect_timeout=10,
                 read_timeout=60, max_request_retries=3,
                 retryable_status_codes=(429, 500, 502, 503, 504),
                 approved_hosts=(), max_pages=100_000, **kwargs):
        super().__init__(**kwargs)
        if max_request_retries < 0:
            raise ValueError('max_request_retries must be a nonnegative integer')
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
        self.max_request_retries = max_request_retries
        self.retryable_status_codes = set(retryable_status_codes)
        self.approved_hosts = tuple(approved_hosts)
        self.max_pages = max_pages

    def _checked_url(self, url):
        parsed = urlsplit(url)
        origin = (parsed.scheme.lower(), parsed.netloc.lower())
        if (parsed.scheme not in ('http', 'https') or parsed.username or parsed.password
                or origin not in self._origins):
            raise ValueError('API URL points to an unapproved origin')
        return url

    def _session(self):
        if not hasattr(self._local, 'session'):
            # Reuse resolved settings, but keep mutable HTTP sessions thread-local.
            session = requests.Session()
            for attribute in ('headers', 'auth', 'cookies', 'params', 'proxies',
                              'verify', 'cert', 'trust_env', 'stream', 'max_redirects'):
                setattr(session, attribute, copy(getattr(self._http_session, attribute)))
            self._local.session = session
            with self._session_lock:
                self._sessions.append(session)
        return self._local.session

    def _request(self, method, url, params):
        self._checked_url(url)
        for attempt in range(self.max_request_retries + 1):
            self._limiter.wait()
            delay = (2 ** attempt) + random.uniform(0, 1)
            try:
                response = self._session().request(
                    method, url, params=params, timeout=self.timeout, allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt == self.max_request_retries:
                    raise
                self._limiter.cooldown(delay)
                continue
            with response:
                if (response.status_code not in self.retryable_status_codes
                        or attempt == self.max_request_retries):
                    response.raise_for_status()
                    # Redirects are disabled so credentials cannot leave approved origins.
                    if 300 <= response.status_code < 400:
                        raise requests.HTTPError('API redirect refused', response=response)
                    return response.json()
                if response.status_code == 429 and response.headers.get('Retry-After'):
                    value = response.headers['Retry-After']
                    try:
                        delay = float(value)
                    except ValueError:
                        delay = parsedate_to_datetime(value).timestamp() - time.time()
                self._limiter.cooldown(max(0, delay))

    def _pages(self, endpoint):
        pagination = endpoint.get('pagination') or {}
        strategy = pagination.get('strategy')
        if strategy not in (None, 'page', 'offset', 'cursor', 'url'):
            raise ValueError(f'Unknown pagination strategy: {strategy}')
        params = dict(endpoint.get('params') or {})
        params.update(pagination.get('params') or {})
        identifier = pagination['identifier'] if strategy in ('page', 'offset', 'cursor') else None
        if strategy in ('page', 'offset'):
            params[identifier] = pagination.get('start', 1 if strategy == 'page' else 0)
        url = self._checked_url(urljoin(self._base_url, endpoint['path']))
        seen = set()
        for _ in range(self.max_pages):
            state = (url, json.dumps(params, sort_keys=True))
            if state in seen:
                raise ValueError('Pagination repeated a cursor or URL')
            seen.add(state)
            payload = self._request(endpoint.get('request', 'GET'), url, params)
            records = json_path(payload, endpoint.get('records_path'))
            if not isinstance(records, list):
                raise ValueError('Collection must be a JSON array; configure records_path')
            yield records
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
                    if not records:
                        raise ValueError('Empty page claims more records; extraction is incomplete')
                elif not records:
                    return
                params[identifier] += 1 if strategy == 'page' else pagination['step']
            else:
                path = pagination['cursor_path' if strategy == 'cursor' else 'next_url_path']
                next_value = json_path(payload, path, optional=True)
                if next_value is None:
                    return
                if not records:
                    raise ValueError('Empty page has a continuation; extraction is incomplete')
                if strategy == 'cursor':
                    params[identifier] = next_value
                else:
                    url = self._checked_url(urljoin(url, next_value))
                    params = {}
        raise ValueError('Pagination exceeded max_pages; extraction is incomplete')

    def _detail(self, endpoint, identifier):
        path = endpoint['detail_path'].replace('{id}', quote(str(identifier), safe=''))
        record = self._request(endpoint.get('request', 'GET'),
                               urljoin(self._base_url, path), endpoint.get('detail_params'))
        if not isinstance(record, dict) or not record:
            raise ValueError('Detail response must be a nonempty JSON object')
        return record

    def _extract(self, endpoint, destination, executor):
        seen = set()
        with destination.open('w', encoding='utf-8') as output:
            for records in self._pages(endpoint):
                # Only one worker-sized batch of futures is ever outstanding.
                for start in range(0, len(records), self.max_workers):
                    batch = records[start:start + self.max_workers]
                    if endpoint.get('detail_path'):
                        ids = []
                        for item in batch:
                            value = json_path(item, endpoint.get('id_field'))
                            if type(value) not in (str, int) or value == '':
                                raise ValueError('Collection requires valid identifiers; configure id_field')
                            if str(value) in seen:
                                raise ValueError('Duplicate collection identifier')
                            seen.add(str(value))
                            ids.append(value)
                        results = executor.map(lambda value: self._detail(endpoint, value), ids)
                    else:
                        results = batch
                    for record in results:
                        if record is None:
                            raise ValueError('Missing JSON record')
                        output.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + '\n')
        self.log.info('Extracted endpoint %s', endpoint['name'])

    def execute(self, context):
        config = yaml.safe_load(Path(self.config_path).read_text(encoding='utf-8'))
        endpoints = config['end_points']
        if self.endpoint_name is not None:
            endpoints = [endpoint for endpoint in endpoints if endpoint['name'] == self.endpoint_name]
            if len(endpoints) != 1:
                raise ValueError(f'Expected one endpoint named {self.endpoint_name}')
        names = set()
        for endpoint in endpoints:
            name = endpoint['name']
            if not re.fullmatch(r'[A-Za-z0-9_-]+', name) or name in names:
                raise ValueError('Endpoint names must be unique and contain letters, digits, _ or -')
            names.add(name)
            if endpoint.get('detail_path') and '{id}' not in endpoint['detail_path']:
                raise ValueError('detail_path requires {id}')
        hook = HttpHook(http_conn_id=self.connection_id)
        self._http_session = hook.get_conn()
        self._base_url = hook.url_from_endpoint('').rstrip('/') + '/'
        self._origins = {(urlsplit(url).scheme.lower(), urlsplit(url).netloc.lower())
                         for url in (self._base_url, *self.approved_hosts)}
        self._limiter = _RateLimiter(self.requests_per_second)
        self._local = threading.local()
        self._local.session = self._http_session
        self._session_lock = threading.Lock()
        self._sessions = [self._http_session]
        interval = f"{context['data_interval_start'].isoformat()}_{context['data_interval_end'].isoformat()}"
        prefix = self.gcs_prefix.strip('/')
        paths = []
        if self.local_output_dir:
            run_folder = re.sub(r'[^A-Za-z0-9_.-]', '_', context.get('run_id') or interval)
            directory = Path(self.local_output_dir) / run_folder / self.task_id
            directory.mkdir(parents=True, exist_ok=True)
            staging = nullcontext(directory)
        else:
            staging = TemporaryDirectory()
        try:
            with staging as directory, ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for endpoint in endpoints:
                    path = Path(directory) / (endpoint['name'] + '.jsonl')
                    self.log.info('Writing local extraction to %s', path)
                    self._extract(endpoint, path, executor)
                    paths.append((path, '/'.join(filter(None, (prefix, endpoint['name'], interval + '.jsonl')))))
                # No output is uploaded until every endpoint has passed validation.
                gcs = GCSHook(gcp_conn_id=self.gcp_conn_id)
                for path, object_name in paths:
                    gcs.upload(bucket_name=self.gcs_bucket, object_name=object_name,
                               filename=str(path), mime_type='application/x-ndjson')
                return [f'gs://{self.gcs_bucket}/{name}' for _, name in paths]
        finally:
            for session in self._sessions:
                session.close()
