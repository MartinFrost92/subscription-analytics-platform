# Synthetic source API

Start with `bash scripts/setup.sh` from the repository root. Explore endpoints
at http://localhost:8000/docs (Airflow uses `http://source-api:8000`).

`GET /invoices?page=1&page_size=100` returns only invoice IDs, ordered by
invoice number. Pages start at 1; page size defaults to 100 and is limited to
1–1,000. `X-Total-Count` gives the total; follow the `Link` header's `rel="next"`
URL until the header is absent. Pages beyond the end return `[]`. Invalid
pagination parameters return HTTP 422.

Retrieve each record using `GET /invoices/{invoice_number}`. Customers and
subscriptions keep their unpaginated identifier lists. Detail requests retain
the configurable 100 ms delay. Pagination does not modify stored data.
