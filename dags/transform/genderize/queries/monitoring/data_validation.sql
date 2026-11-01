WITH totals AS (
    SELECT
        'names_row_count' AS test_name,
        CAST((SELECT COUNT(*) FROM `{{ params.project_id }}.{{ params.datasets.transform }}.names`) AS NUMERIC) AS output_value,
        CAST((SELECT COUNT(*) FROM `{{ params.project_id }}.{{ params.datasets.staging }}.names`) AS NUMERIC) AS source_value
)
SELECT
    test_name,
    output_value,
    source_value,
    CASE
        WHEN output_value = source_value THEN NUMERIC '0'
        ELSE SAFE_DIVIDE(output_value - source_value, source_value) * NUMERIC '100'
    END AS diff
FROM totals
