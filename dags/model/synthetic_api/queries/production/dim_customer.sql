SELECT DISTINCT
    account_id AS customer_key,
    CAST(account_id AS STRING) AS account_id,
    account_name,
    account_country,
    gender,
    account_created_at AS created_at
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
