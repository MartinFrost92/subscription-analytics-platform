WITH subscription_details AS (
    SELECT
        subscriptions.subscription_id,
        subscriptions.account_id,
        subscriptions.start_date,
        subscriptions.stop_date,
        customers.name AS account_name,
        customers.country AS account_country,
        subscriptions.product,
        subscriptions.status AS current_status,
        GREATEST(
            COALESCE(customers.updated_at, subscriptions.updated_at),
            subscriptions.updated_at
        ) AS updated_at
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.subscriptions` AS subscriptions
    LEFT JOIN `{{ params.project_id }}.{{ params.datasets.staging }}.customers` AS customers USING (account_id)
    WHERE subscriptions.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
        OR customers.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
        OR NOT EXISTS (
            SELECT 1
            FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
        )
)
SELECT
    start_date AS date,
    account_id,
    subscription_id,
    account_name,
    account_country,
    product,
    current_status,
    updated_at,
    TRUE AS is_sale
FROM subscription_details

UNION ALL

SELECT
    stop_date AS date,
    account_id,
    subscription_id,
    account_name,
    account_country,
    product,
    current_status,
    updated_at,
    FALSE AS is_sale
FROM subscription_details
WHERE stop_date IS NOT NULL
