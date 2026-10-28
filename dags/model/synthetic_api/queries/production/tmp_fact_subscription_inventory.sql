SELECT
    date,
    account_id AS customer_key,
    FARM_FINGERPRINT(CAST(product AS STRING)) AS product_key,
    subscription_id AS subscription_key,
    is_sale,
    updated_at
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
WHERE updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
    OR NOT EXISTS (
        SELECT 1
        FROM `{{ params.project_id }}.{{ params.datasets.model }}.fact_subscription_inventory`
    )
