MERGE `{{ params.project_id }}.{{ params.datasets.staging }}.invoices` AS target
USING `{{ params.project_id }}.{{ params.datasets.utilities }}.{{ data_interval_end | ts_nodash }}_{{ dag.dag_id }}_invoices` AS source
ON target.invoice_number = source.invoice_number
WHEN MATCHED THEN
    UPDATE SET
        account_id = source.account_id,
        subscription_id = source.subscription_id,
        charge = source.charge,
        discount = source.discount,
        start_date = source.start_date,
        stop_date = source.stop_date,
        updated_at = DATE('{{ data_interval_end | ds }}')
WHEN NOT MATCHED THEN
    INSERT (invoice_number, account_id, subscription_id, charge, discount, start_date, stop_date, updated_at)
    VALUES (
        source.invoice_number,
        source.account_id,
        source.subscription_id,
        source.charge,
        source.discount,
        source.start_date,
        source.stop_date,
        DATE('{{ data_interval_end | ds }}')
    );
