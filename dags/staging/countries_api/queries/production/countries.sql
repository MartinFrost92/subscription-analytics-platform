SELECT country_id
FROM `{{ params.project_id }}.{{ params.datasets.staging }}.countries`
WHERE is_checked = FALSE
ORDER BY country_id;
