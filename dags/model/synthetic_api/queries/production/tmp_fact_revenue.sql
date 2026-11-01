SELECT
    revenue.date,
    customer.customer_key,
    product.product_key,
    subscription.subscription_key,
    invoice.invoice_key,
    revenue.daily_revenue,
    revenue.updated_at
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue` AS revenue
LEFT JOIN `{{ params.project_id }}.{{ params.datasets.model }}.dim_customer` AS customer
    ON CAST(revenue.account_id AS STRING) = customer.account_id
LEFT JOIN `{{ params.project_id }}.{{ params.datasets.model }}.dim_product` AS product
    ON revenue.product = product.product
LEFT JOIN `{{ params.project_id }}.{{ params.datasets.model }}.dim_subscription` AS subscription
    ON CAST(revenue.subscription_id AS STRING) = subscription.subscription_id
LEFT JOIN `{{ params.project_id }}.{{ params.datasets.model }}.dim_invoice` AS invoice
    ON CAST(revenue.invoice_number AS STRING) = invoice.invoice_number
WHERE revenue.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
    OR NOT EXISTS (SELECT 1 FROM `{{ params.project_id }}.{{ params.datasets.model }}.fact_revenue`)
