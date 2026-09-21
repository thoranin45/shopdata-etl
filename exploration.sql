-- =====================================================================
-- exploration.sql
-- Purpose: profile raw views in shopdata.db and uncover data quality issues
-- =====================================================================

-- ---------------------------------------------------------------------
-- Section 0: Schema & basic profiling
-- ---------------------------------------------------------------------

-- 0.1 List all objects and how each view is defined
SELECT type, name, sql FROM sqlite_master WHERE type IN ('view', 'table');

-- 0.2 Column structure of each view
PRAGMA table_info(vw_raw_customers);
PRAGMA table_info(vw_raw_orders);
PRAGMA table_info(vw_exchange_rates);

-- 0.3 Sample rows
SELECT * FROM vw_raw_customers LIMIT 10;
SELECT * FROM vw_raw_orders LIMIT 10;
SELECT * FROM vw_exchange_rates LIMIT 10;

-- 0.4 Row counts (baseline for later comparison)
SELECT 'customers' AS view_name, COUNT(*) AS row_count FROM vw_raw_customers
UNION ALL
SELECT 'orders', COUNT(*) FROM vw_raw_orders
UNION ALL
SELECT 'exchange_rates', COUNT(*) FROM vw_exchange_rates;

-- ---------------------------------------------------------------------
-- Section 1: vw_raw_customers anomalies
-- ---------------------------------------------------------------------

-- 1.1 Duplicate customer_id (same customer appears more than once)
SELECT customer_id,
       COUNT(*)                         AS record_count,
       GROUP_CONCAT(signup_date, ' | ') AS signup_dates
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 1.2 Missing or blank emails, and emails with an invalid shape
SELECT COUNT(*)                                   AS total_rows,
       SUM(email IS NULL)                         AS null_email,
       SUM(TRIM(email) = '')                      AS blank_email,
       SUM(email IS NOT NULL
           AND TRIM(email) <> ''
           AND email NOT LIKE '%_@_%._%')         AS malformed_email
FROM vw_raw_customers;

-- 1.3 Phone number patterns
SELECT customer_id,
       phone,
       CASE
           WHEN phone IS NULL OR TRIM(phone) = '' THEN 'missing'
           WHEN phone GLOB '*[A-Za-z]*'           THEN 'contains letters'
           WHEN phone GLOB '*[^0-9]*'             THEN 'has formatting characters'
           ELSE 'digits only'
       END AS phone_pattern
FROM vw_raw_customers
ORDER BY phone_pattern, customer_id;

-- 1.4 signup_date that is missing or not a valid ISO date (YYYY-MM-DD)
SELECT customer_id, signup_date
FROM vw_raw_customers
WHERE signup_date IS NULL
   OR date(signup_date) IS NULL
   OR date(signup_date) <> signup_date;

-- 1.5 full_name missing or with leading/trailing spaces
SELECT customer_id, '[' || full_name || ']' AS full_name_raw
FROM vw_raw_customers
WHERE full_name IS NULL
   OR full_name <> TRIM(full_name);

-- ---------------------------------------------------------------------
-- Section 2: vw_raw_orders anomalies
-- ---------------------------------------------------------------------

-- 2.1 Orders with zero, negative, or missing amounts
SELECT order_id, customer_id, total_amount, currency, status
FROM vw_raw_orders
WHERE total_amount IS NULL
   OR total_amount <= 0
ORDER BY total_amount;

-- 2.2 Status breakdown (status is not mentioned in the cleaning rules)
SELECT status,
       COUNT(*)               AS order_count,
       SUM(total_amount <= 0) AS non_positive_amounts
FROM vw_raw_orders
GROUP BY status
ORDER BY order_count DESC;

-- 2.3 Distinct currency values, bracketed to reveal whitespace/casing issues
SELECT '[' || COALESCE(currency, 'NULL') || ']' AS currency_raw,
       COUNT(*)                                 AS order_count
FROM vw_raw_orders
GROUP BY currency
ORDER BY order_count DESC;

-- 2.4 Orphan orders: customer_id does not exist in vw_raw_customers
SELECT o.order_id, o.customer_id, o.total_amount, o.currency
FROM vw_raw_orders AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM vw_raw_customers AS c
    WHERE c.customer_id = o.customer_id
);

-- 2.5 Duplicate order_id
SELECT order_id, COUNT(*) AS record_count
FROM vw_raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 2.6 order_date missing or not a valid ISO date
SELECT order_id, order_date
FROM vw_raw_orders
WHERE order_date IS NULL
   OR date(order_date) IS NULL
   OR date(order_date) <> order_date;

-- ---------------------------------------------------------------------
-- Section 3: vw_exchange_rates coverage
-- ---------------------------------------------------------------------

-- 3.1 Date range covered per currency
SELECT currency,
       MIN(date) AS first_date,
       MAX(date) AS last_date,
       COUNT(*)  AS rate_days
FROM vw_exchange_rates
GROUP BY currency;

-- 3.2 Duplicate rates for the same currency and date (would duplicate orders on join)
SELECT currency, date, COUNT(*) AS rate_count
FROM vw_exchange_rates
GROUP BY currency, date
HAVING COUNT(*) > 1;

-- 3.3 Non-USD orders with no matching exchange rate on their order_date
SELECT o.order_id, o.order_date, o.currency, o.total_amount
FROM vw_raw_orders AS o
LEFT JOIN vw_exchange_rates AS r
       ON r.currency = o.currency
      AND r.date     = o.order_date
WHERE o.currency IS NOT NULL
  AND o.currency <> 'USD'
  AND r.rate_to_usd IS NULL;
