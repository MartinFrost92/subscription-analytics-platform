MERGE `{{ params.project_id }}.{{ params.datasets.staging }}.subscriptions` AS target
USING `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_subscriptions` AS source
ON target.subscription_id = source.subscription_id
WHEN MATCHED AND (
    target.account_id IS DISTINCT FROM source.account_id
    OR target.product IS DISTINCT FROM source.product
    OR target.start_date IS DISTINCT FROM source.start_date
    OR target.stop_date IS DISTINCT FROM source.stop_date
    OR target.status IS DISTINCT FROM source.status
) THEN
    UPDATE SET
        account_id = source.account_id,
        product = source.product,
        start_date = source.start_date,
        stop_date = source.stop_date,
        status = source.status,
        updated_at = DATE('{{ data_interval_end | ds }}')
WHEN NOT MATCHED THEN
    INSERT (subscription_id, account_id, product, start_date, stop_date, status, updated_at)
    VALUES (
        source.subscription_id,
        source.account_id,
        source.product,
        source.start_date,
        source.stop_date,
        source.status,
        DATE('{{ data_interval_end | ds }}')
    );
