"""A fixed EUR-denominated monthly billing snapshot as of 2026-01-01."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import random

CUTOFF = date(2026, 1, 1)


def month_offset(value, months):
    index = value.year * 12 + value.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


def generate():
    rng = random.Random(42)
    first_names = ('Anna', 'James', 'Sofia', 'Oliver', 'Emma', 'Noah', 'Mia', 'Lucas')
    last_names = ('Jensen', 'Smith', 'Garcia', 'Martin', 'Andersson', 'Muller', 'Rossi')
    customers = []
    for account_id in range(1, 3_001):
        customers.append((
            account_id, f'{rng.choice(first_names)} {rng.choice(last_names)}',
            rng.choice(('DK', 'SE', 'DE', 'GB', 'FR', 'NL', 'ES', 'IT')),
            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(days=rng.randrange(365)),
        ))
    subscriptions, invoices = [], []
    # Each customer has three subscriptions, one per product.
    products = (('basic', Decimal('19.00')), ('standard', Decimal('49.00')),
                ('premium', Decimal('99.00')))
    for subscription_id in range(1, 9_001):
        account_id = (subscription_id - 1) % 3_000 + 1
        product, price = products[(subscription_id - 1) // 3_000]
        months = rng.choice((2, 2, 3, 3, 4))
        cancelled = rng.random() < 0.3
        end = month_offset(CUTOFF, -rng.randint(1, 10)) if cancelled else CUTOFF
        start = month_offset(end, -months)
        subscriptions.append((subscription_id, account_id, product, start,
                              end if cancelled else None,
                              'cancelled' if cancelled else 'active'))
        discount = (price * rng.choice((Decimal('0'), Decimal('0'), Decimal('0.1'),
                                       Decimal('0.2')))).quantize(Decimal('0.01'))
        for period in range(months):
            invoices.append((len(invoices) + 1, account_id, subscription_id,
                             price, discount, month_offset(start, period),
                             month_offset(start, period + 1)))
    return customers, subscriptions, invoices
