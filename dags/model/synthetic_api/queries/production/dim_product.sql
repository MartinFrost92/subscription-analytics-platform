SELECT DISTINCT
    FARM_FINGERPRINT(CAST(product AS STRING)) AS product_key,
    product
FROM `{{ params.project_id }}.{{ params.datasets.staging }}.subscriptions`
