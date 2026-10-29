DELETE FROM `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue`
WHERE invoice_number IN (
    SELECT invoice_number
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_invoice_revenue`
);

INSERT INTO `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue`
    (date, invoice_number, account_id, subscription_id, start_date, stop_date,
     charge, discount, currency, daily_revenue, updated_at)
SELECT
    date, invoice_number, account_id, subscription_id, start_date, stop_date,
    charge, discount, currency, daily_revenue, updated_at
FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_invoice_revenue`;
