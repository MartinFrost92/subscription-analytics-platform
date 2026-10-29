WITH exchange_rates AS (
    SELECT
        _code AS currency,
        _rate AS rate,
        updated_at
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.exchange_rates`

    UNION ALL

    SELECT
        'DKK' AS currency,
        NUMERIC '100' AS rate,
        MAX(updated_at) AS updated_at
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.exchange_rates`
)
SELECT
    staged.currency AS from_currency,
    joined.currency AS to_currency,
    staged.rate / joined.rate AS conversion_rate,
    GREATEST(staged.updated_at, joined.updated_at) AS updated_at
FROM exchange_rates AS staged
CROSS JOIN exchange_rates AS joined
WHERE staged.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
    OR joined.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
    OR NOT EXISTS (
        SELECT 1 FROM `{{ params.project_id }}.{{ params.datasets.transform }}.exchange_rates`
    )
