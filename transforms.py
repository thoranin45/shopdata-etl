"""Pure data-cleaning functions for the ShopData ETL pipeline.

Every function here takes pandas objects in and returns new pandas objects out.
None of them touch a database or Prefect, which keeps the business logic easy to
unit test in isolation. Prefect tasks in pipeline.py wrap these functions.
"""
import pandas as pd

UNKNOWN_EMAIL = "unknown@domain.com"


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

def standardize_phone(phones: pd.Series) -> pd.Series:
    """Remove every non-numeric character from phone numbers.

    Example: "+1 (555) 123-4567" -> "15551234567".
    Values that are missing, or contain no digits at all, become <NA>.
    """
    digits = phones.astype("string").str.replace(r"\D", "", regex=True)
    return digits.replace("", pd.NA)


def fill_missing_emails(emails: pd.Series, placeholder: str = UNKNOWN_EMAIL) -> pd.Series:
    """Trim emails and replace missing or blank values with a placeholder."""
    cleaned = emails.astype("string").str.strip()
    return cleaned.replace("", pd.NA).fillna(placeholder)


def deduplicate_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """Keep one record per customer_id: the one with the most recent signup_date.

    Tie-breaking rules:
    - Same signup_date: keep the row that appears last in the source,
      assumed to be the most recently written record.
    - Missing or unparseable signup_date: treated as the oldest, so a dated
      record always wins over an undated one.

    The input DataFrame is not modified.
    """
    return (
        customers
        .assign(
            _signup_ts=pd.to_datetime(customers["signup_date"], errors="coerce", format="ISO8601"),
            _row_order=range(len(customers)),
        )
        # Oldest first, NaT at the very top, so keep="last" picks the newest record
        .sort_values(["_signup_ts", "_row_order"], na_position="first")
        .drop_duplicates(subset="customer_id", keep="last")
        .drop(columns=["_signup_ts", "_row_order"])
        .sort_values("customer_id")
        .reset_index(drop=True)
    )


def clean_customers(raw_customers: pd.DataFrame) -> pd.DataFrame:
    """Apply all customer cleaning rules: deduplicate, standardize phones, fill emails."""
    customers = deduplicate_customers(raw_customers)
    return customers.assign(
        phone=standardize_phone(customers["phone"]),
        email=fill_missing_emails(customers["email"]),
    )


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

# Values for the fx_rate_source column: how usd_amount was obtained
FX_NATIVE_USD = "native_usd"    # order was already in USD
FX_EXACT = "exact"              # converted with the rate for that currency and order_date
FX_ASSUMED_USD = "assumed_usd"  # currency or rate missing, amount assumed to be USD (per spec)


def filter_invalid_amounts(orders: pd.DataFrame) -> pd.DataFrame:
    """Drop orders whose total_amount is zero, negative or missing.

    Zero and negative amounts are system errors (per spec). Missing amounts are
    dropped too, because an order without an amount has no value to report.
    """
    return orders.loc[orders["total_amount"] > 0].reset_index(drop=True)


def normalize_currency(currencies: pd.Series) -> pd.Series:
    """Trim and upper-case currency codes; blank values become <NA>."""
    cleaned = currencies.astype("string").str.strip().str.upper()
    return cleaned.replace("", pd.NA)


def prepare_exchange_rates(rates: pd.DataFrame) -> pd.DataFrame:
    """Normalize the exchange-rate table and drop rows that cannot be joined safely.

    Rows with a missing currency, missing date or non-positive rate are removed.
    This matters because pandas merge treats missing keys as equal, so a rate
    with a missing date would otherwise match every order with a missing date.
    """
    prepared = rates.assign(
        currency=normalize_currency(rates["currency"]),
        rate_date=pd.to_datetime(rates["date"], errors="coerce", format="ISO8601"),
    )
    is_valid = (
        prepared["currency"].notna()
        & prepared["rate_date"].notna()
        & (prepared["rate_to_usd"] > 0)
    )
    return prepared.loc[is_valid, ["currency", "rate_date", "rate_to_usd"]]


def convert_to_usd(orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Add usd_amount, exchange_rate and fx_rate_source columns.

    usd_amount = total_amount * rate_to_usd for the order's currency on its order_date.
    If the currency is missing or has no rate for that date, the amount is assumed
    to already be USD (per spec) and flagged as 'assumed_usd'.

    Raises pandas.errors.MergeError if the rates contain more than one rate for the
    same currency and date, since that would silently duplicate orders.
    """
    orders_keyed = orders.assign(
        currency=normalize_currency(orders["currency"]),
        _fx_date=pd.to_datetime(orders["order_date"], errors="coerce", format="ISO8601"),
    )
    rates_keyed = prepare_exchange_rates(rates).rename(columns={"rate_date": "_fx_date"})

    merged = orders_keyed.merge(
        rates_keyed,
        how="left",
        on=["currency", "_fx_date"],
        validate="many_to_one",
    )

    is_usd = merged["currency"].eq("USD").fillna(False).astype(bool)
    has_rate = merged["rate_to_usd"].notna() & ~is_usd

    applied_rate = merged["rate_to_usd"].where(has_rate, 1.0)
    fx_rate_source = (
        pd.Series(FX_ASSUMED_USD, index=merged.index)
        .mask(has_rate, FX_EXACT)
        .mask(is_usd, FX_NATIVE_USD)
    )

    return merged.assign(
        exchange_rate=applied_rate,
        usd_amount=(merged["total_amount"] * applied_rate).round(2),
        fx_rate_source=fx_rate_source,
    ).drop(columns=["_fx_date", "rate_to_usd"])


def clean_orders(raw_orders: pd.DataFrame, raw_rates: pd.DataFrame) -> pd.DataFrame:
    """Apply all order cleaning rules: drop invalid amounts, then convert to USD.

    Orphan orders and the status column are intentionally kept; see README.
    """
    valid_orders = filter_invalid_amounts(raw_orders)
    return convert_to_usd(valid_orders, raw_rates)
