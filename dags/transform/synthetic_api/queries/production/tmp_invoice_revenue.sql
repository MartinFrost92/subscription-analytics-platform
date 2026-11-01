WITH invoice_amounts AS (
    SELECT
        invoices.* EXCEPT(updated_at),
        subscriptions.product,
        GREATEST(invoices.updated_at, COALESCE(subscriptions.updated_at, invoices.updated_at)) AS updated_at,
        charge - discount AS net_revenue,
        DATE_DIFF(invoices.stop_date, invoices.start_date, DAY) AS number_of_days,
        SAFE_DIVIDE(charge - discount, DATE_DIFF(invoices.stop_date, invoices.start_date, DAY)) AS daily_amount
    FROM `{{ params.project_id }}.{{ params.datasets.staging }}.invoices` AS invoices
    LEFT JOIN `{{ params.project_id }}.{{ params.datasets.staging }}.subscriptions` AS subscriptions
        USING(subscription_id)
    WHERE invoices.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
        OR subscriptions.updated_at >= DATE_SUB(DATE('{{ data_interval_end | ds }}'), INTERVAL {{ params.variables.date_window }} DAY)
        OR NOT EXISTS (
            SELECT 1
            FROM `{{ params.project_id }}.{{ params.datasets.transform }}.invoice_revenue`
        )
)
SELECT
    revenue_date AS date,
    invoice_number,
    account_id,
    subscription_id,
    product,
    start_date,
    stop_date,
    charge,
    discount,
    'EUR' AS currency,
    CASE
        WHEN revenue_date = DATE_SUB(stop_date, INTERVAL 1 DAY)
            THEN net_revenue - daily_amount * (number_of_days - 1)
        ELSE daily_amount
    END AS daily_revenue,
    updated_at
FROM invoice_amounts,
-- Stop dates are exclusive; the final day absorbs the rounding remainder.
UNNEST(GENERATE_DATE_ARRAY(start_date, DATE_SUB(stop_date, INTERVAL 1 DAY))) AS revenue_date
