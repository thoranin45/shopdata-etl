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