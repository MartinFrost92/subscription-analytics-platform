MERGE `{{ params.project_id }}.{{ params.datasets.staging }}.names` AS target
USING (
    SELECT DISTINCT
        LOWER(REGEXP_EXTRACT(TRIM(name), r'^[^ ]+')) AS name,
        UPPER(TRIM(country)) AS country_id
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.customers`
    WHERE NULLIF(TRIM(name), '') IS NOT NULL
        AND NULLIF(TRIM(country), '') IS NOT NULL
) AS source
ON target.name = source.name
    AND target.country_id = source.country_id
WHEN NOT MATCHED THEN
    INSERT (name, country_id, is_checked, updated_at)
    VALUES (source.name, source.country_id, FALSE, DATE('{{ data_interval_end | ds }}'));
