DELETE FROM `{{ params.project_id }}.{{ params.datasets.transform }}.exchange_rates` AS target
WHERE EXISTS (
    SELECT 1
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_exchange_rates` AS source
    WHERE target.from_currency = source.from_currency
        AND target.to_currency = source.to_currency
);

INSERT INTO `{{ params.project_id }}.{{ params.datasets.transform }}.exchange_rates`
    (from_currency, to_currency, conversion_rate, updated_at)
SELECT from_currency, to_currency, conversion_rate, updated_at
FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_exchange_rates`;
