SELECT
    name AS first_name,
    country_id
FROM `{{ params.project_id }}.{{ params.datasets.staging }}.names`
WHERE is_checked = FALSE;