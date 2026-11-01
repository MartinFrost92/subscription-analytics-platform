SELECT
    subscription_id AS subscription_key,
    CAST(subscription_id AS STRING) AS subscription_id,
    MIN(IF(is_sale, date, NULL)) AS start_date,
    MAX(IF(NOT is_sale, date, NULL)) AS stop_date,
    ARRAY_AGG(current_status ORDER BY updated_at DESC, date DESC LIMIT 1)[OFFSET(0)] AS current_status
FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
GROUP BY subscription_id
