WITH staging_rates AS (
    SELECT
        _code AS currency,
        _rate / NUMERIC '100' AS dkk_rate
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.exchange_rates`
    WHERE _code != 'DKK'
),
transformed_rates AS (
    SELECT
        from_currency AS currency,
        conversion_rate AS dkk_rate
    FROM `{{ params.project_id }}.{{ params.datasets.transform }}.exchange_rates`
    WHERE to_currency = 'DKK'
        AND from_currency != 'DKK'
),
comparisons AS (
    SELECT
        staging.currency AS source_currency,
        transformed.currency AS output_currency,
        staging.dkk_rate AS source_rate,
        transformed.dkk_rate AS output_rate
    FROM staging_rates AS staging
    FULL OUTER JOIN transformed_rates AS transformed USING (currency)
),
violations AS (
    SELECT
        'dkk_currency_completeness' AS test_name,
        COUNTIF(source_currency IS NULL OR output_currency IS NULL) AS violation_count
    FROM comparisons

    UNION ALL

    SELECT
        'dkk_exchange_rate_exact_match' AS test_name,
        COUNTIF(
            source_currency IS NOT NULL
            AND output_currency IS NOT NULL
            AND output_rate IS DISTINCT FROM source_rate
        ) AS violation_count
    FROM comparisons
)
SELECT
    test_name,
    CAST(violation_count AS NUMERIC) AS output_value,
    NUMERIC '0' AS source_value,
    CAST(violation_count AS NUMERIC) AS diff
FROM violations
