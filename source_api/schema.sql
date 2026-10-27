CREATE TABLE IF NOT EXISTS customers (
    account_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES customers(account_id),
    product TEXT NOT NULL,
    start_date DATE NOT NULL,
    stop_date DATE,
    status TEXT NOT NULL CHECK (status IN ('active', 'cancelled')),
    CHECK (stop_date > start_date),
    CHECK ((status = 'active') = (stop_date IS NULL)),
    UNIQUE (subscription_id, account_id)
);
CREATE TABLE IF NOT EXISTS invoices (
    invoice_number INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES customers(account_id),
    subscription_id INTEGER NOT NULL,
    charge NUMERIC(12,2) NOT NULL CHECK (charge > 0),
    discount NUMERIC(12,2) NOT NULL CHECK (discount >= 0 AND discount <= charge),
    start_date DATE NOT NULL,
    stop_date DATE NOT NULL CHECK (stop_date > start_date),
    FOREIGN KEY (subscription_id, account_id)
        REFERENCES subscriptions(subscription_id, account_id),
    UNIQUE (subscription_id, start_date)
);
CREATE INDEX IF NOT EXISTS subscriptions_account_idx ON subscriptions(account_id);
CREATE INDEX IF NOT EXISTS invoices_account_idx ON invoices(account_id);
COMMENT ON COLUMN invoices.stop_date IS 'Exclusive end of the billed period.';
