WITH totals AS (
    {% for endpoint in params.end_points %}
    SELECT
        '{{ endpoint.name }}_row_count' AS test_name,
        CAST((
            SELECT COUNT(*)
            FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_{{ endpoint.name }}`
        ) AS NUMERIC) AS output_value,
        CAST((
            SELECT expected_rows
            FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_{{ params.name }}_metadata`
            WHERE name = '{{ endpoint.name }}'
        ) AS NUMERIC) AS expected_rows
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),
comparison AS (
    SELECT
        test_name,
        output_value,
        COALESCE(expected_rows, output_value) AS source_value
    FROM totals
)
SELECT
    test_name,
    output_value,
    source_value,
    CASE
        WHEN source_value = 0 AND output_value = 0 THEN NUMERIC '0'
        WHEN source_value = 0 THEN NULL
        ELSE SAFE_DIVIDE(output_value - source_value, source_value) * NUMERIC '100'
    END AS diff
FROM comparison
