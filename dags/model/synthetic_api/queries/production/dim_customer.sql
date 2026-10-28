SELECT
    account_id AS customer_key,
    CAST(account_id AS STRING) AS account_id,
    name AS account_name,
    country AS account_country,
    created_at
FROM `{{ params.project_id }}.{{ params.datasets.staging }}.customers`
