-- Existing table creation skips schema changes, so add the nullable field on upgrades.
ALTER TABLE `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
ADD COLUMN IF NOT EXISTS gender STRING;

ALTER TABLE `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
ADD COLUMN IF NOT EXISTS account_created_at TIMESTAMP;

ALTER TABLE `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
ADD COLUMN IF NOT EXISTS start_date DATE;

ALTER TABLE `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
ADD COLUMN IF NOT EXISTS stop_date DATE;

DELETE FROM `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
WHERE subscription_id IN (
    SELECT subscription_id
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_subscription_inventory`
);

INSERT INTO `{{ params.project_id }}.{{ params.datasets.transform }}.subscription_inventory`
    (date, start_date, stop_date, account_id, subscription_id, account_name, account_country, account_created_at, gender,
     product, current_status, updated_at, is_sale)
SELECT date, start_date, stop_date, account_id, subscription_id, account_name, account_country, account_created_at, gender,
       product, current_status, updated_at, is_sale
FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_subscription_inventory`;
