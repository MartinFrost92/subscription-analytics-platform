SELECT
    name,
    country_id,
    CASE
        WHEN probability >= NUMERIC '0.95' AND NULLIF(TRIM(gender), '') IS NOT NULL
            THEN INITCAP(TRIM(gender))
        ELSE 'Other'
    END AS gender,
    updated_at
FROM `{{ params.project_id }}.{{ params.datasets.staging }}.names`
WHERE updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
    OR NOT EXISTS (
        SELECT 1 FROM `{{ params.project_id }}.{{ params.datasets.transform }}.names`
    )
