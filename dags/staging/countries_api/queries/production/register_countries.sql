MERGE `{{ params.project_id }}.{{ params.datasets.staging }}.countries` AS target
USING (
    SELECT DISTINCT UPPER(TRIM(country)) AS country_id
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.customers`
    WHERE NULLIF(TRIM(country), '') IS NOT NULL
) AS source
ON target.country_id = source.country_id
WHEN NOT MATCHED THEN
    INSERT (country_id, is_checked, updated_at)
    VALUES (source.country_id, FALSE, DATE('{{ data_interval_end | ds }}'));
