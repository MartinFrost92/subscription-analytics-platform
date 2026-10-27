import asyncio
from contextlib import asynccontextmanager
import logging
import math
import os
from pathlib import Path
import secrets

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, Security
from fastapi.security import APIKeyHeader
import psycopg
from psycopg.rows import dict_row

from generate import generate

LATENCY_MS = float(os.getenv('SOURCE_DETAIL_LATENCY_MS', '100'))
if not math.isfinite(LATENCY_MS) or LATENCY_MS < 0:
    raise ValueError('SOURCE_DETAIL_LATENCY_MS must be a finite nonnegative number')


def connect():
    return psycopg.connect(
        host=os.getenv('SOURCE_DB_HOST', 'source-db'),
        port=os.getenv('SOURCE_DB_PORT', '5432'),
        dbname=os.getenv('SOURCE_DB_NAME', 'subscription_source'),
        user=os.getenv('SOURCE_DB_USER', 'source'),
        password=os.environ['SOURCE_DB_PASSWORD'],
        connect_timeout=10,
    )


def initialize():
    with connect() as conn:
        # Serialize initialization if multiple API processes start together.
        conn.execute('SELECT pg_advisory_xact_lock(734218)')
        conn.execute(Path(__file__).with_name('schema.sql').read_text())
        populated = conn.execute('''SELECT
            EXISTS (SELECT 1 FROM customers),
            EXISTS (SELECT 1 FROM subscriptions),
            EXISTS (SELECT 1 FROM invoices)''').fetchone()
        if any(populated):
            logging.info('Existing source data preserved; skipping seed.')
            return
        for table, rows in zip(('customers', 'subscriptions', 'invoices'), generate()):
            with conn.cursor().copy(f'COPY {table} FROM STDIN') as copy:
                for row in rows:
                    copy.write_row(row)
            logging.info('Seeded %s: %s records', table, len(rows))


@asynccontextmanager
async def lifespan(app):
    api_key = os.environ.get('SOURCE_API_KEY')
    if not api_key:
        raise RuntimeError('SOURCE_API_KEY must be configured')
    app.state.api_key = api_key.encode('utf-8')
    await asyncio.to_thread(initialize)
    yield


app = FastAPI(title='Synthetic subscription source', lifespan=lifespan)

api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)


def authenticate(request: Request, key: str | None = Security(api_key_header)):
    if key is None or not secrets.compare_digest(key.encode('utf-8'), request.app.state.api_key):
        raise HTTPException(status_code=401, detail='Invalid or missing API key')


@app.get('/health', include_in_schema=False)
def health():
    with connect() as conn:
        conn.execute('SELECT 1')
    return {'status': 'ok'}


def identifiers(table, key):
    with connect() as conn:
        return [row[0] for row in conn.execute(f'SELECT {key} FROM {table} ORDER BY {key}')]


async def detail(table, key, value):
    await asyncio.sleep(LATENCY_MS / 1000)

    def fetch():
        with connect() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                return cursor.execute(f'SELECT * FROM {table} WHERE {key} = %s',
                                      (value,)).fetchone()

    record = await asyncio.to_thread(fetch)
    if record is None:
        raise HTTPException(status_code=404, detail='Record not found')
    return record


@app.get('/customers', dependencies=[Depends(authenticate)])
def customers():
    return identifiers('customers', 'account_id')


@app.get('/customers/{account_id}', dependencies=[Depends(authenticate)])
async def customer(account_id: int):
    return await detail('customers', 'account_id', account_id)


@app.get('/subscriptions', dependencies=[Depends(authenticate)])
def subscriptions():
    return identifiers('subscriptions', 'subscription_id')


@app.get('/subscriptions/{subscription_id}', dependencies=[Depends(authenticate)])
async def subscription(subscription_id: int):
    return await detail('subscriptions', 'subscription_id', subscription_id)


@app.get('/invoices', dependencies=[Depends(authenticate)])
def invoices(request: Request, response: Response,
             page: int = Query(default=1, ge=1, le=2_147_483_647),
             page_size: int = Query(default=100, ge=1, le=1000)):
    """Return invoice IDs in order; follow the Link header for the next page."""
    offset = (page - 1) * page_size
    with connect() as conn:
        total = conn.execute('SELECT count(*) FROM invoices').fetchone()[0]
        rows = conn.execute(
            'SELECT invoice_number FROM invoices ORDER BY invoice_number LIMIT %s OFFSET %s',
            (page_size, offset),
        ).fetchall()
    response.headers['X-Total-Count'] = str(total)
    if offset + page_size < total:
        next_url = request.url.include_query_params(page=page + 1, page_size=page_size)
        response.headers['Link'] = f'<{next_url}>; rel="next"'
    return [row[0] for row in rows]


@app.get('/invoices/{invoice_number}', dependencies=[Depends(authenticate)])
async def invoice(invoice_number: int):
    return await detail('invoices', 'invoice_number', invoice_number)
