MERGE `{{ params.project_id }}.{{ params.datasets.staging }}.names` AS target
USING (
    SELECT
        LOWER(TRIM(name)) AS name,
        UPPER(TRIM(country_id)) AS country_id,
        count,
        gender,
        probability
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_names`
) AS source
ON target.name = source.name
    AND target.country_id = source.country_id
WHEN MATCHED THEN
    UPDATE SET
        count = source.count,
        gender = source.gender,
        probability = source.probability,
        is_checked = TRUE,
        updated_at = DATE('{{ data_interval_end | ds }}')
WHEN NOT MATCHED THEN
    INSERT (name, country_id, count, gender, probability, is_checked, updated_at)
    VALUES (
        source.name,
        source.country_id,
        source.count,
        source.gender,
        source.probability,
        TRUE,
        DATE('{{ data_interval_end | ds }}')
    );
