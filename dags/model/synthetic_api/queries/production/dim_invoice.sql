SELECT
    invoice_number AS invoice_key,
    CAST(invoice_number AS STRING) AS invoice_number,
    start_date,
    stop_date,
    charge,
    discount
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue`
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY invoice_number
    ORDER BY updated_at DESC, date DESC
) = 1
