SELECT
    date,
    CONCAT(FORMAT_DATE('%B', date), ' ', EXTRACT(YEAR FROM date)) AS month_name_year,
    CAST(CONCAT(EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)) AS INTEGER) AS year_month_sort,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date) AS STRING)) AS quarter,
    EXTRACT(QUARTER FROM date) AS quarter_number,
    EXTRACT(YEAR FROM date) AS year
FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2030-12-31')) AS date
ORDER BY
    date;
