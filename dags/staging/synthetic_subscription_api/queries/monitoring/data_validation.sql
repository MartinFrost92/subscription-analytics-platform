WITH totals AS (
    {% for endpoint in params.end_points %}
    SELECT
        '{{ endpoint.name }}_row_count' AS test_name,
        (
            SELECT CAST(COUNT(*) AS NUMERIC)
            FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_{{ endpoint.name }}`
        ) AS output_value,
        (
            SELECT CAST(expected_rows AS NUMERIC)
            FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_metadata`
            WHERE name = '{{ endpoint.name }}'
        ) AS expected_rows
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
        WHEN output_value = source_value THEN 0
        ELSE SAFE_DIVIDE(output_value - source_value, source_value) * 100
    END AS diff
FROM comparison
