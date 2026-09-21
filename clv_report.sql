-- =====================================================================
-- clv_report.sql
-- Purpose: Customer Lifetime Value (CLV) per customer from the cleaned
--          analytics tables (dim_customers, fct_orders).
--
-- Business rules (see README, Data Quality Findings):
--   * total_orders_placed counts every valid order the customer placed,
--     whatever its status.
--   * lifetime_value_usd sums usd_amount of COMPLETED orders only, since
--     cancelled and pending orders have not generated revenue.
--   * Every customer appears, including customers with no orders (value 0).
--   * Orders for unknown customers (orphans) are excluded because the report
--     starts from dim_customers.
--   * assumed_fx_value_usd shows how much of the lifetime value relies on the
--     "missing rate = USD" rule, so the BI team can judge its reliability.
-- =====================================================================

WITH order_totals AS (
    -- Aggregate orders once per customer before joining
    SELECT
        customer_id,
        COUNT(*) AS total_orders_placed,
        SUM(CASE WHEN status = 'COMPLETED' THEN usd_amount ELSE 0 END) AS lifetime_value_usd,
        SUM(CASE WHEN status = 'COMPLETED' AND fx_rate_source = 'assumed_usd'
                 THEN usd_amount ELSE 0 END) AS assumed_fx_value_usd
    FROM fct_orders
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.full_name,
    COALESCE(t.total_orders_placed, 0)                             AS total_orders_placed,
    ROUND(COALESCE(t.lifetime_value_usd, 0), 2)                    AS lifetime_value_usd,
    strftime('%Y-%m', c.signup_date)                               AS customer_cohort,
    ROUND(COALESCE(t.assumed_fx_value_usd, 0), 2)                  AS assumed_fx_value_usd,
    RANK() OVER (ORDER BY COALESCE(t.lifetime_value_usd, 0) DESC)  AS clv_rank
FROM dim_customers AS c
LEFT JOIN order_totals AS t
       ON t.customer_id = c.customer_id
ORDER BY clv_rank, c.customer_id;
