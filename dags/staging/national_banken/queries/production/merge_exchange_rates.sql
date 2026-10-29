MERGE `{{ params.project_id }}.{{ params.datasets.staging }}.exchange_rates` AS target
USING `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_exchange_rates` AS source
ON target._code = source._code
WHEN MATCHED THEN
    UPDATE SET
        _desc = source._desc,
        _rate = source._rate,
        updated_at = DATE('{{ data_interval_end | ds }}')
WHEN NOT MATCHED THEN
    INSERT (_code, _desc, _rate, updated_at)
    VALUES (
        source._code,
        source._desc,
        source._rate,
        DATE('{{ data_interval_end | ds }}')
    );
