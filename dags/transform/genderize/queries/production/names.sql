DELETE FROM `{{ params.project_id }}.{{ params.datasets.transform }}.names` AS target
WHERE EXISTS (
    SELECT 1
    FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_names` AS source
    WHERE target.name = source.name
        AND target.country_id = source.country_id
);

INSERT INTO `{{ params.project_id }}.{{ params.datasets.transform }}.names`
    (name, country_id, gender, updated_at)
SELECT name, country_id, gender, updated_at
FROM `{{ params.project_id }}.{{ params.datasets.utilities }}.tmp_names`;
