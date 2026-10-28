SELECT
    revenue.date,
    revenue.account_id AS customer_key,
    FARM_FINGERPRINT(CAST(subscriptions.product AS STRING)) AS product_key,
    revenue.subscription_id AS subscription_key,
    revenue.invoice_number AS invoice_key,
    revenue.daily_revenue,
    GREATEST(revenue.updated_at, subscriptions.updated_at) AS updated_at
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue` AS revenue
JOIN `{{ params.project_id }}.{{ params.datasets.staging }}.subscriptions` AS subscriptions USING (subscription_id)
WHERE revenue.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
    OR subscriptions.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
    OR NOT EXISTS (SELECT 1 FROM `{{ params.project_id }}.{{ params.datasets.model }}.fact_revenue`)
