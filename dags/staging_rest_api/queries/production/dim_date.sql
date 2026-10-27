SELECT
    date,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(MONTH FROM date) AS month_number,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date) AS STRING)) AS quarter,
    EXTRACT(QUARTER FROM date) AS quarter_number,
    EXTRACT(YEAR FROM date) AS year
FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE '2025-01-01', DATE '2030-12-31')) AS date
ORDER BY
    date;
