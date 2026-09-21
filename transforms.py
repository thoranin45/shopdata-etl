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
