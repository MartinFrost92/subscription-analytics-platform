WITH totals AS (
    SELECT
        (SELECT COALESCE(SUM(daily_revenue), NUMERIC '0')
         FROM `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue`)
            AS output_value,
        (SELECT COALESCE(SUM(charge - discount), NUMERIC '0')
         FROM `{{ params.project_id }}.{{ params.datasets.staging }}.invoices`)
            AS source_value
)
SELECT
    'invoice_revenue_reconciliation' AS test_name,
    output_value,
    source_value,
    CASE
        WHEN output_value = source_value THEN NUMERIC '0'
        WHEN source_value = 0 THEN NULL
        -- Enforce exact totals even if the percentage rounds to zero in NUMERIC.
        WHEN SAFE_DIVIDE(output_value - source_value, source_value) * 100 = 0
            THEN ERROR('Invoice revenue totals differ, but the percentage difference rounds to zero.')
        ELSE SAFE_DIVIDE(output_value - source_value, source_value) * 100
    END AS diff
FROM totals

UNION ALL

SELECT
    'subscription_inventory_reconciliation' AS test_name,
    output_value,
    source_value,
    CASE
        WHEN output_value = source_value THEN NUMERIC '0'
        WHEN source_value = 0 THEN NULL
        WHEN SAFE_DIVIDE(output_value - source_value, source_value) * 100 = 0
            THEN ERROR('Subscription inventory counts differ, but the percentage difference rounds to zero.')
        ELSE SAFE_DIVIDE(output_value - source_value, source_value) * 100
    END AS diff
FROM (
    SELECT
        CAST((SELECT COUNT(*)
              FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`)
             AS NUMERIC) AS output_value,
        CAST((SELECT COUNT(start_date) + COUNT(stop_date)
              FROM `{{ params.project_id }}.{{ params.datasets.staging }}.subscriptions`)
             AS NUMERIC) AS source_value
)
