from datetime import date
from decimal import Decimal
import json
import os
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from generate import CUTOFF, generate


class GeneratorTests(unittest.TestCase):
    def test_reproducibility_and_relationships(self):
        data = generate()
        self.assertEqual(data, generate())
        customers, subscriptions, invoices = data
        self.assertEqual(len(customers), 3_000)
        self.assertEqual(len(subscriptions), 9_000)
        self.assertTrue(24_000 < len(invoices) < 26_000)
        accounts = {row[0]: row for row in customers}
        contracts = {row[0]: row for row in subscriptions}
        last_end = {}
        for sub_id, account, _, start, stop, status in subscriptions:
            self.assertLessEqual(accounts[account][3].date(), start)
            self.assertEqual(stop is None, status == 'active')
        for _, account, sub_id, charge, discount, start, stop in invoices:
            sub = contracts[sub_id]
            self.assertEqual(account, sub[1])
            self.assertGreaterEqual(start, sub[3])
            self.assertLessEqual(stop, sub[4] or CUTOFF)
            self.assertLess(start, stop)
            self.assertIsInstance(charge, Decimal)
            self.assertTrue(Decimal(0) <= discount <= charge)
            if sub_id in last_end:
                self.assertEqual(last_end[sub_id], start)
            last_end[sub_id] = stop


class EndpointTests(unittest.TestCase):
    def open(self, path):
        url = path if path.startswith('http://') else 'http://localhost:8000' + path
        return urlopen(Request(url, headers={'X-API-Key': os.environ['SOURCE_API_KEY']}), timeout=20)

    def get(self, path):
        with self.open(path) as response:
            return json.load(response)

    def test_authentication(self):
        with urlopen('http://localhost:8000/health', timeout=20) as response:
            self.assertEqual(response.status, 200)
        for path in ('/customers', '/customers/1', '/subscriptions', '/subscriptions/1',
                     '/invoices', '/invoices/1'):
            for headers in ({}, {'X-API-Key': os.environ['SOURCE_API_KEY'] + '-incorrect'}):
                with self.subTest(path=path, headers_present=bool(headers)):
                    with self.assertRaises(HTTPError) as error:
                        urlopen(Request('http://localhost:8000' + path, headers=headers), timeout=20)
                    self.assertEqual(error.exception.code, 401)
            self.get(path)

    def test_collections_and_details(self):
        for resource, key, minimum in (
            ('customers', 'account_id', 3_000),
            ('subscriptions', 'subscription_id', 9_000),
            ('invoices', 'invoice_number', 100),
        ):
            with self.subTest(resource=resource):
                identifiers = self.get('/' + resource)
                self.assertGreaterEqual(len(identifiers), minimum)
                self.assertTrue(all(type(value) is int for value in identifiers))
                self.assertEqual(identifiers, sorted(set(identifiers)))
                detail = self.get(f'/{resource}/{identifiers[0]}')
                self.assertEqual(detail[key], identifiers[0])
                self.assertGreater(len(detail), 1)
                with self.assertRaises(HTTPError) as error:
                    self.get(f'/{resource}/-1')
                self.assertEqual(error.exception.code, 404)
        invoice = self.get('/invoices/1')
        subscription = self.get(f'/subscriptions/{invoice["subscription_id"]}')
        customer = self.get(f'/customers/{invoice["account_id"]}')
        self.assertEqual(subscription['account_id'], customer['account_id'])
        self.assertLess(date.fromisoformat(invoice['start_date']),
                        date.fromisoformat(invoice['stop_date']))

    def test_invoice_pagination(self):
        from app import connect

        with connect() as conn:
            expected = [row[0] for row in conn.execute(
                'SELECT invoice_number FROM invoices ORDER BY invoice_number')]
        with self.open('/invoices') as response:
            self.assertEqual(json.load(response), expected[:100])
            self.assertEqual(int(response.headers['X-Total-Count']), len(expected))
            self.assertIn('page=2', response.headers['Link'])
        url = 'http://localhost:8000/invoices?page=1&page_size=1000'
        collected = []
        while url:
            with self.open(url) as response:
                batch = json.load(response)
                self.assertTrue(0 < len(batch) <= 1000)
                collected.extend(batch)
                link = response.headers.get('Link')
                url = link.split('>')[0][1:] if link else None
        self.assertEqual(collected, expected)
        with self.open('/invoices?page=2147483647') as response:
            self.assertEqual(json.load(response), [])
            self.assertIsNone(response.headers.get('Link'))
        for query in ('page=0', 'page=-1', 'page=abc', 'page_size=0', 'page_size=1001'):
            with self.subTest(query=query), self.assertRaises(HTTPError) as error:
                self.get('/invoices?' + query)
            self.assertEqual(error.exception.code, 422)

    def test_initialization_preserves_existing_records(self):
        from app import connect, initialize

        with connect() as conn:
            before = conn.execute('SELECT * FROM customers ORDER BY account_id').fetchall()
            counts_before = [conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                             for table in ('customers', 'subscriptions', 'invoices')]
        initialize()
        with connect() as conn:
            self.assertEqual(before, conn.execute('SELECT * FROM customers ORDER BY account_id').fetchall())
            self.assertEqual(counts_before, [conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                             for table in ('customers', 'subscriptions', 'invoices')])
