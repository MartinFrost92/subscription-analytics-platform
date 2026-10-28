MERGE `{{ params.project_id }}.{{ params.datasets.staging }}.customers` AS target
USING `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_customers` AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN
    UPDATE SET
        name = source.name,
        country = source.country,
        created_at = source.created_at,
        updated_at = DATE('{{ data_interval_end | ds }}')
WHEN NOT MATCHED THEN
    INSERT (account_id, name, country, created_at, updated_at)
    VALUES (
        source.account_id,
        source.name,
        source.country,
        source.created_at,
        DATE('{{ data_interval_end | ds }}')
    );
