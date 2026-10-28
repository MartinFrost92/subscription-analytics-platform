DELETE FROM `{{ params.project_id }}.{{ params.datasets.model }}.fact_revenue`
WHERE invoice_key IN (
    SELECT invoice_key FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_fact_revenue`
);

INSERT INTO `{{ params.project_id }}.{{ params.datasets.model }}.fact_revenue`
    (date, customer_key, product_key, subscription_key, invoice_key, daily_revenue, updated_at)
SELECT date, customer_key, product_key, subscription_key, invoice_key, daily_revenue, updated_at
FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_fact_revenue`;
