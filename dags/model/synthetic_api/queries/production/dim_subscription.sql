SELECT DISTINCT
    subscription_id AS subscription_key,
    CAST(subscription_id AS STRING) AS subscription_id,
    start_date,
    stop_date,
    current_status
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`