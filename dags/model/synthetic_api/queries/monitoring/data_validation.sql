WITH totals AS (
    SELECT
        'revenue_reconciliation' AS test_name,
        (SELECT COALESCE(SUM(daily_revenue), NUMERIC '0') FROM `{{ params.project_id }}.{{ params.datasets.model }}.fact_revenue`) AS output_value,
        (SELECT COALESCE(SUM(daily_revenue), NUMERIC '0') FROM `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue`) AS source_value

    UNION ALL

    SELECT
        'subscription_inventory_row_count',
        (SELECT COUNT(*) FROM `{{ params.project_id }}.{{ params.datasets.model }}.fact_subscription_inventory`),
        (SELECT COUNT(*) FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`)
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
