DELETE FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
WHERE subscription_id IN (
    SELECT subscription_id
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_subscription_inventory`
);

INSERT INTO `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
    (date, account_id, subscription_id, account_name, account_country,
     product, current_status, updated_at, is_sale)
SELECT date, account_id, subscription_id, account_name, account_country,
       product, current_status, updated_at, is_sale
FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_subscription_inventory`;
