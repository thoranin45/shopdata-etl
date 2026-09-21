# ShopData ETL Pipeline

An ETL pipeline that extracts raw order-management data from `shopdata.db`, cleans it, and loads it
into `analytics.db` so the BI team can report on Customer Lifetime Value (CLV).

Built with Python 3.12, pandas, Prefect 3 and SQLite.

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

## Exploring the source data

`run_sql.py` runs every statement in a `.sql` file against a SQLite database (opened read-only)
and prints each result:

```bash
python run_sql.py exploration.sql
```

## Data Quality Findings

Queries are in `exploration.sql`; section numbers below refer to that file.

Baseline row counts: 12 customer rows (10 unique customers), 20 orders, 15 exchange-rate rows.

### Issues found

**1. Duplicate customers (1.1).**
Customers 1 and 2 each appear twice with different `signup_date` values, and the duplicate
records disagree on contact details (e.g. customer 1 has two different emails and phone formats).
*Handling:* keep the record with the most recent `signup_date`, as specified.

**2. Missing emails (1.2).**
2 customer rows have a NULL email. No blank-string emails or malformed addresses were found.
*Handling:* replace missing emails with `unknown@domain.com` after deduplication.

**3. Inconsistent and invalid phone numbers (1.3).**
- Mixed formats: `+1 (555) 123-4567`, `555-987-6543`, `(555) 333 4444`, `+44 20 7123 1234`.
- Values containing letters: `Ext 444` (an extension, not a phone number) and `1-800-555-DINO`
  (a vanity number).
- Inconsistent country codes: after cleaning, numbers have 10, 11 or 12 digits.
- 2 customers have no phone number.

*Handling:* strip all non-numeric characters, as specified. Note that `Ext 444` becomes `444` and
`1-800-555-DINO` becomes `1800555`, neither of which is a dialable number. Normalising country
codes is out of scope but recommended as a follow-up.

**4. Zero and negative order amounts (2.1, 2.2).**
3 orders have `total_amount <= 0`: 103 (-50.00 USD) and 113 (-100.00 EUR) with status
`SYSTEM_ERROR`, and 114 (0.00 USD) with status `COMPLETED`. Order 114 shows that filtering on
status alone would not catch every invalid amount.
*Handling:* remove all orders with `total_amount <= 0`.

**5. Missing currency (2.3).**
2 orders have a NULL currency. Existing currency codes are consistent (no casing or whitespace
issues).
*Handling:* treat as USD, as specified.

**6. Exchange-rate coverage gap (3.1, 3.3).**
Rates exist only for EUR, GBP and JPY from 2023-05-01 to 2023-05-05, but orders run until
2023-05-14. 6 non-USD orders have no matching rate: 110, 111, 113, 115, 118, 120.
Under the specified rule these amounts are treated as USD, which distorts CLV significantly.
For example, order 115 (25,000 JPY) becomes 25,000 USD, compared with roughly 175 USD at the
last available JPY rate.
*Handling:* follow the specified rule, and add an `fx_rate_source` column to `fct_orders`
(`native_usd`, `exact`, `assumed_usd`) so the BI team can identify unreliable amounts.
*Recommendation:* use the most recent available rate on or before the order date (as-of join).

**7. Orphan orders (2.4).**
Orders 106 and 118 belong to `customer_id` 99, which does not exist in `vw_raw_customers`.
*Handling:* kept in `fct_orders` because they are real transactions, but excluded from the CLV
report, which is built from `dim_customers`.

**8. Missing order date (2.6).**
Order 117 has a NULL `order_date`, so no exchange rate can be matched to it.
*Handling:* kept; converted using the missing-rate rule.

**9. Undocumented `status` column (2.2).**
Values: `COMPLETED` (16), `SYSTEM_ERROR` (2), `PENDING` (1), `CANCELLED` (1). The cleaning rules
do not mention status.
*Handling:* kept in `fct_orders`; the CLV report counts `COMPLETED` orders only, since cancelled
and pending orders have not generated revenue.

### Checks that passed

- `signup_date` values are all valid ISO dates (`YYYY-MM-DD`); `order_date` is valid except order 117.
- No missing names or names with leading/trailing whitespace.
- No duplicate `order_id`.
- No duplicate exchange rate for the same currency and date, so joining on (currency, date)
  cannot duplicate orders.
- `rate_to_usd` is expressed as USD per unit of currency (EUR = 1.10), so
  `usd_amount = total_amount * rate_to_usd`.
