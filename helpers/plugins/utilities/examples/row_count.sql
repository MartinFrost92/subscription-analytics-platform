WITH totals AS (
    SELECT
        CAST((SELECT COUNT(*) FROM
            `{{ params.project_id }}.{{ params.output_dataset }}.{{ params.output_table }}`)
            AS NUMERIC) AS output_value,
        CAST((SELECT COUNT(*) FROM
            `{{ params.project_id }}.{{ params.source_dataset }}.{{ params.source_table }}`)
            AS NUMERIC) AS source_value
)
SELECT
    'row_count_reconciliation' AS test_name,
    output_value,
    source_value,
    CASE
        WHEN source_value = 0 AND output_value = 0 THEN NUMERIC '0'
        WHEN source_value = 0 THEN NULL
        ELSE SAFE_DIVIDE(output_value - source_value, source_value) * NUMERIC '100'
    END AS diff
FROM totals
