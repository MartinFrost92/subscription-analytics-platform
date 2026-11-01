WITH totals AS (
    SELECT
        'exchange_rates_row_count',
        (SELECT COUNT(*) FROM `{{ params.project_id }}.{{ params.datasets.model }}.exchange_rates`),
        (SELECT COUNT(*) FROM `{{ params.project_id }}.{{ params.datasets.transform }}.exchange_rates`)
)

SELECT
    test_name,
    output_value,
    source_value,
    CASE
        WHEN output_value = source_value THEN 0
        ELSE SAFE_DIVIDE(output_value - source_value, source_value) * 100
    END AS diff
FROM totals
