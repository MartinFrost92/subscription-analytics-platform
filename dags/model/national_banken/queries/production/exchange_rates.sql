SELECT
    from_currency,
    to_currency,
    conversion_rate
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.exchange_rates`
