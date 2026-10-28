SELECT
    invoice_number AS invoice_key,
    CAST(invoice_number AS STRING) AS invoice_number,
    start_date,
    stop_date,
    charge,
    discount
FROM `{{ params.project_id }}.{{ params.datasets.staging }}.invoices`
