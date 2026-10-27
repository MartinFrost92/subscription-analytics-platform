MERGE `{{ params.project_id }}.{{ params.datasets.model }}.fact_sales` AS target
USING (
    WITH sales AS (
        SELECT
            DATE('{{ data_interval_end | ds }}') AS date,
            customer_id,
            CAST(FLOOR(RAND() * 1951) AS INT64) + 50 AS amount
        FROM
            UNNEST(GENERATE_ARRAY(1, 50)) AS customer_id
        WHERE
            RAND() < 0.20
    )
    SELECT
        dim_date.date,
        dim_customer.customer_id,
        SUM(sales.amount) AS sales
    FROM
        sales
    INNER JOIN
        `{{ params.project_id }}.{{ params.datasets.model }}.dim_date` AS dim_date
        ON sales.date = dim_date.date
    INNER JOIN
        `{{ params.project_id }}.{{ params.datasets.model }}.dim_customer` AS dim_customer
        ON sales.customer_id = dim_customer.customer_id
    GROUP BY
        ALL
) AS source
ON
    target.date = DATE('{{ data_interval_end | ds }}')
    AND target.date = source.date
    AND target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET sales = source.sales
WHEN NOT MATCHED THEN
    INSERT (date, customer_id, sales)
    VALUES (source.date, source.customer_id, source.sales);
