# ShopData ETL Pipeline

An ETL pipeline that extracts raw order-management data from `shopdata.db`, cleans it, and loads it
into `analytics.db` so the BI team can report on Customer Lifetime Value (CLV).

Built with Python 3.12, pandas, Prefect 3, pytest and SQLite.

## Project structure

| File | Purpose |
|---|---|
| `exploration.sql` | Part 1: queries that profile the raw views and uncover data quality issues |
| `transforms.py` | Cleaning rules as pure pandas functions (no database or Prefect dependency) |
| `pipeline.py` | Part 2: Prefect flow that extracts, transforms and loads the data |
| `tests/` | Part 3: unit tests for the cleaning rules, using in-memory DataFrames |
| `clv_report.sql` | Part 4: CLV report built on the cleaned tables |
| `run_sql.py` | Helper that runs every statement in a `.sql` file (database opened read-only) |

The cleaning logic is kept in `transforms.py`, separate from orchestration in `pipeline.py`, so it
can be tested without a database.

## Setup

```bash
python -m venv .venv

# Windows (PowerShell)
.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

Place the provided `shopdata.db` in the project root.

## Running the pipeline

```bash
python pipeline.py
```

The flow `shopdata-etl` runs these tasks:

1. `extract_view` (x3): reads each source view read-only; retries twice on transient errors.
2. `transform_customers` / `transform_orders`: apply the cleaning rules and log what changed.
3. `check_orphan_orders`: warns about orders whose customer is not in `dim_customers`.
4. `load_table` (x2): writes `dim_customers` and `fct_orders` to `analytics.db`. Tables are replaced
   on every run, so the pipeline is safe to re-run. If the database cannot be written, the data is
   saved to `clean_customers.csv` and `clean_orders.csv` instead.

To browse runs in the Prefect UI, start `prefect server start` in another terminal, open
http://127.0.0.1:4200, then run the pipeline.

### Output tables

**`dim_customers`**: one row per customer (unique index on `customer_id`).
`customer_id`, `full_name`, `email`, `phone` (digits only), `signup_date` (`YYYY-MM-DD`).

**`fct_orders`**: one row per valid order (unique index on `order_id`, index on `customer_id`).
Source columns plus:

| Column | Meaning |
|---|---|
| `exchange_rate` | Rate applied to convert to USD (1.0 when no conversion was made) |
| `usd_amount` | `total_amount * exchange_rate`, rounded to 2 decimals |
| `fx_rate_source` | `native_usd` (already USD), `exact` (rate found for that currency and date), or `assumed_usd` (currency or rate missing, amount assumed to be USD as specified) |

## Running the tests

```bash
pytest -v
```

Tests cover phone standardization, email filling, deduplication (including ties and missing dates),
amount filtering, currency normalization and USD conversion (including missing rates, missing
dates and duplicate rates). They build small DataFrames in memory and never touch `shopdata.db`.

## CLV report

After running the pipeline:

```bash
python run_sql.py clv_report.sql analytics.db
```

Output columns: `customer_id`, `full_name`, `total_orders_placed`, `lifetime_value_usd`,
`customer_cohort` (signup month, e.g. `2023-01`), plus two additions:

- `assumed_fx_value_usd`: the part of lifetime value that relies on the missing-rate rule.
- `clv_rank`: rank by lifetime value (ties share a rank).

Rules: every customer is included (customers with no orders show 0); `total_orders_placed` counts
all valid orders; `lifetime_value_usd` counts `COMPLETED` orders only; orders for unknown customers
are excluded.

With the provided data, the top-ranked customer (customer 3) owes all of their value to order 115,
a JPY order with no exchange rate that is counted as 25,000 USD. This is why
`assumed_fx_value_usd` is included in the report.

## Data Quality Findings

Queries are in `exploration.sql`; section numbers below refer to that file.

Baseline row counts: 12 customer rows (10 unique customers), 20 orders, 15 exchange-rate rows.
After cleaning: 10 rows in `dim_customers`, 17 rows in `fct_orders`.

### Issues found

**1. Duplicate customers (1.1).**
Customers 1 and 2 each appear twice with different `signup_date` values, and the duplicate
records disagree on contact details. For customer 2, only the newer record has an email.
*Handling:* keep the record with the most recent `signup_date`, as specified. Ties keep the row
that appears last in the source; a record with a missing date never wins over a dated one.

**2. Missing emails (1.2).**
2 customer rows have a NULL email. No blank-string emails or malformed addresses were found.
*Handling:* replace missing or blank emails with `unknown@domain.com` after deduplication.
One customer (8) ends up with the placeholder.

**3. Inconsistent and invalid phone numbers (1.3).**
- Mixed formats: `+1 (555) 123-4567`, `555-987-6543`, `(555) 333 4444`, `+44 20 7123 1234`.
- Values containing letters: `Ext 444` (an extension, not a phone number) and `1-800-555-DINO`
  (a vanity number).
- Inconsistent country codes: after cleaning, numbers have 10, 11 or 12 digits.
- 2 customers have no phone number.

*Handling:* strip all non-numeric characters, as specified; values with no digits become NULL.
`Ext 444` becomes `444` and `1-800-555-DINO` becomes `1800555`, neither of which is dialable.
Normalizing country codes is out of scope but recommended as a follow-up.

**4. Zero and negative order amounts (2.1, 2.2).**
3 orders have `total_amount <= 0`: 103 (-50.00 USD) and 113 (-100.00 EUR) with status
`SYSTEM_ERROR`, and 114 (0.00 USD) with status `COMPLETED`. Order 114 shows that filtering on
status alone would not catch every invalid amount.
*Handling:* remove all orders with `total_amount <= 0` (or a missing amount).

**5. Missing currency (2.3).**
2 orders have a NULL currency. Existing currency codes are consistent (no casing or whitespace
issues).
*Handling:* treat as USD, as specified, flagged `assumed_usd`. Codes are still trimmed and
upper-cased defensively before matching rates.

**6. Exchange-rate coverage gap (3.1, 3.3).**
Rates exist only for EUR, GBP and JPY from 2023-05-01 to 2023-05-05, but orders run until
2023-05-14. After removing invalid amounts, 5 non-USD orders have no matching rate:
110, 111, 115, 118, 120. Under the specified rule these amounts are treated as USD, which distorts
CLV significantly. For example, order 115 (25,000 JPY) becomes 25,000 USD, compared with roughly
175 USD at the last available JPY rate.
*Handling:* follow the specified rule and flag these orders `assumed_usd` in `fct_orders`; the
pipeline also logs a warning listing them.
*Recommendation:* use the most recent available rate on or before the order date (as-of join),
and extend the rate feed to cover every order date.

**7. Orphan orders (2.4).**
Orders 106 and 118 belong to `customer_id` 99, which does not exist in `vw_raw_customers`.
*Handling:* kept in `fct_orders` because they are real transactions, and logged as a warning.
They are excluded from the CLV report, which is built from `dim_customers`.

**8. Missing order date (2.6).**
Order 117 has a NULL `order_date`. It is a USD order, so its conversion is unaffected.
*Handling:* kept. The conversion logic never matches a missing order date to any rate.

**9. Undocumented `status` column (2.2).**
Values: `COMPLETED` (16), `SYSTEM_ERROR` (2), `PENDING` (1), `CANCELLED` (1). The cleaning rules
do not mention status.
*Handling:* kept in `fct_orders`; the CLV report counts only `COMPLETED` orders toward lifetime
value, since cancelled and pending orders have not generated revenue.

### Checks that passed

- `signup_date` values are all valid ISO dates (`YYYY-MM-DD`); `order_date` is valid except order 117.
- No missing names or names with leading/trailing whitespace.
- No duplicate `order_id`.
- No duplicate exchange rate for the same currency and date. The pipeline enforces this at join
  time and fails loudly if it is ever violated, rather than duplicating orders.
- `rate_to_usd` is expressed as USD per unit of currency (EUR = 1.10), so
  `usd_amount = total_amount * rate_to_usd`.

## Git workflow

Each part was developed on its own feature branch (`feature/data-exploration`,
`feature/customer-cleaning`, `feature/order-cleaning`, `feature/prefect-pipeline`,
`feature/clv-report`) and merged into `main` with `--no-ff`, so every merge is visible in the
history. Commit messages follow the Conventional Commits style (`feat`, `test`, `docs`, `chore`).
