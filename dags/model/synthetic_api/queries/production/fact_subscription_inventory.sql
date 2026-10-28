DELETE FROM `{{ params.project_id }}.{{ params.datasets.model }}.fact_subscription_inventory`
WHERE subscription_key IN (
    SELECT subscription_key
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_fact_subscription_inventory`
);

INSERT INTO `{{ params.project_id }}.{{ params.datasets.model }}.fact_subscription_inventory`
    (date, customer_key, product_key, subscription_key, is_sale, updated_at)
SELECT date, customer_key, product_key, subscription_key, is_sale, updated_at
FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_fact_subscription_inventory`;
