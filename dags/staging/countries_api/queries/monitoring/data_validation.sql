WITH fetched AS (
    SELECT UPPER(JSON_VALUE(codes, '$.alpha_2')) AS country_id
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_countries`
),
requested AS (
    SELECT country_id
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.countries`
    WHERE is_checked = FALSE
),
totals AS (
    SELECT
        'countries_row_count' AS test_name,
        CAST((SELECT COUNT(*) FROM fetched) AS NUMERIC) AS output_value,
        CAST((
            SELECT expected_rows
            FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_metadata`
            WHERE name = 'countries'
        ) AS NUMERIC) AS source_value
),
violations AS (
    SELECT
        'countries_requested_codes_match' AS test_name,
        COUNTIF(requested.country_id IS NULL OR fetched.country_id IS NULL) AS violation_count
    FROM requested
    FULL OUTER JOIN fetched USING (country_id)

    UNION ALL

    SELECT
        'countries_duplicate_codes',
        COUNT(*) - COUNT(DISTINCT country_id)
    FROM fetched

    UNION ALL

    SELECT
        'countries_valid_codes',
        COUNTIF(country_id IS NULL OR NOT REGEXP_CONTAINS(country_id, r'^[A-Z]{2}$'))
    FROM fetched
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
FROM totals

UNION ALL

SELECT
    test_name,
    CAST(violation_count AS NUMERIC) AS output_value,
    NUMERIC '0' AS source_value,
    CAST(violation_count AS NUMERIC) AS diff
FROM violations;
