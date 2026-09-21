"""Unit tests for customer cleaning logic.

All tests build small in-memory DataFrames, so they never touch shopdata.db.
"""
import pandas as pd
import pytest

from transforms import (
    UNKNOWN_EMAIL,
    clean_customers,
    deduplicate_customers,
    fill_missing_emails,
    standardize_phone,
)

CUSTOMER_COLUMNS = ["customer_id", "full_name", "email", "phone", "signup_date"]


def make_customers(rows: list[tuple]) -> pd.DataFrame:
    """Build a raw customers DataFrame with the same columns as vw_raw_customers."""
    return pd.DataFrame(rows, columns=CUSTOMER_COLUMNS)


# ---------------------------------------------------------------------------
# standardize_phone
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("+1 (555) 123-4567", "15551234567"),  # example from the assignment
        ("555-987-6543", "5559876543"),
        ("(555) 333 4444", "5553334444"),
        ("+44 20 7123 1234", "442071231234"),
        ("1234567890", "1234567890"),          # already clean
        ("1-800-555-DINO", "1800555"),         # letters are dropped
        ("Ext 444", "444"),
    ],
)
def test_standardize_phone_keeps_only_digits(raw, expected):
    assert standardize_phone(pd.Series([raw])).iloc[0] == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "ext"])
def test_standardize_phone_returns_missing_when_no_digits(raw):
    assert pd.isna(standardize_phone(pd.Series([raw])).iloc[0])


# ---------------------------------------------------------------------------
# fill_missing_emails
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", [None, "", "   "])
def test_fill_missing_emails_uses_placeholder(raw):
    assert fill_missing_emails(pd.Series([raw])).iloc[0] == UNKNOWN_EMAIL


def test_fill_missing_emails_keeps_and_trims_real_email():
    assert fill_missing_emails(pd.Series([" alice@example.com "])).iloc[0] == "alice@example.com"


# ---------------------------------------------------------------------------
# deduplicate_customers
# ---------------------------------------------------------------------------

def test_deduplicate_keeps_most_recent_signup():
    raw = make_customers([
        (1, "Alice Smith", "alice@example.com", "+1 (555) 123-4567", "2023-01-15"),
        (2, "Bob Jones", None, "555-987-6543", "2023-02-20"),
        (1, "Alice Smith", "alice.smith@example.com", "15551234567", "2023-06-01"),
    ])

    result = deduplicate_customers(raw)

    assert result["customer_id"].tolist() == [1, 2]
    alice = result.loc[result["customer_id"] == 1].iloc[0]
    assert alice["email"] == "alice.smith@example.com"
    assert alice["signup_date"] == "2023-06-01"


def test_deduplicate_does_not_depend_on_row_order():
    raw = make_customers([
        (1, "A", "new@example.com", None, "2023-06-01"),
        (1, "A", "old@example.com", None, "2023-01-15"),
    ])

    assert deduplicate_customers(raw).iloc[0]["email"] == "new@example.com"


def test_deduplicate_tie_keeps_last_row():
    raw = make_customers([
        (1, "A", "first@example.com", None, "2023-01-15"),
        (1, "A", "second@example.com", None, "2023-01-15"),
    ])

    assert deduplicate_customers(raw).iloc[0]["email"] == "second@example.com"


def test_deduplicate_prefers_dated_record_over_missing_date():
    raw = make_customers([
        (1, "A", "dated@example.com", None, "2023-01-15"),
        (1, "A", "undated@example.com", None, None),
    ])

    assert deduplicate_customers(raw).iloc[0]["email"] == "dated@example.com"


def test_deduplicate_does_not_modify_input():
    raw = make_customers([
        (1, "A", "a@example.com", None, "2023-01-15"),
        (1, "A", "b@example.com", None, "2023-06-01"),
    ])
    original = raw.copy()

    deduplicate_customers(raw)

    pd.testing.assert_frame_equal(raw, original)


# ---------------------------------------------------------------------------
# clean_customers (all rules together)
# ---------------------------------------------------------------------------

def test_clean_customers_applies_all_rules():
    raw = make_customers([
        (2, "Bob Jones", None, "555-987-6543", "2023-02-20"),
        (2, "Bob Jones", "bob@example.com", "555-987-6543", "2023-09-15"),
        (8, "Hannah Abbott", None, None, "2023-07-01"),
    ])

    result = clean_customers(raw).set_index("customer_id")

    assert len(result) == 2
    assert result.loc[2, "email"] == "bob@example.com"   # newest record kept its real email
    assert result.loc[2, "phone"] == "5559876543"
    assert result.loc[8, "email"] == UNKNOWN_EMAIL
    assert pd.isna(result.loc[8, "phone"])
