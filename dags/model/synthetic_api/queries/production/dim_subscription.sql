SELECT
    subscription_id AS subscription_key,
    CAST(subscription_id AS STRING) AS subscription_id,
    start_date,
    stop_date,
    status AS current_status
FROM `{{ params.project_id }}.{{ params.datasets.staging }}.subscriptions`
