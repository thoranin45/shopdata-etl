"""Unit tests for order cleaning and currency conversion logic.

All tests build small in-memory DataFrames, so they never touch shopdata.db.
"""
import pandas as pd
import pytest

from transforms import (
    FX_ASSUMED_USD,
    FX_EXACT,
    FX_NATIVE_USD,
    clean_orders,
    convert_to_usd,
    filter_invalid_amounts,
    normalize_currency,
)

ORDER_COLUMNS = ["order_id", "customer_id", "order_date", "total_amount", "currency", "status"]
RATE_COLUMNS = ["currency", "rate_to_usd", "date"]


def make_orders(rows: list[tuple]) -> pd.DataFrame:
    """Build a raw orders DataFrame with the same columns as vw_raw_orders."""
    return pd.DataFrame(rows, columns=ORDER_COLUMNS)


def make_rates(rows: list[tuple]) -> pd.DataFrame:
    """Build an exchange-rate DataFrame with the same columns as vw_exchange_rates."""
    return pd.DataFrame(rows, columns=RATE_COLUMNS)


RATES = make_rates([
    ("EUR", 1.10, "2023-05-01"),
    ("JPY", 0.0070, "2023-05-01"),
    ("GBP", 1.25, "2023-05-01"),
])


def convert_single(order_date, amount, currency, rates=RATES) -> pd.Series:
    """Convert one order and return its result row."""
    orders = make_orders([(1, 1, order_date, amount, currency, "COMPLETED")])
    return convert_to_usd(orders, rates).iloc[0]


# ---------------------------------------------------------------------------
# filter_invalid_amounts
# ---------------------------------------------------------------------------

def test_filter_invalid_amounts_removes_zero_negative_and_missing():
    orders = make_orders([
        (101, 1, "2023-05-01", 150.00, "USD", "COMPLETED"),
        (103, 3, "2023-05-01", -50.00, "USD", "SYSTEM_ERROR"),
        (114, 10, "2023-05-01", 0.00, "USD", "COMPLETED"),
        (200, 2, "2023-05-01", None, "USD", "COMPLETED"),
        (201, 2, "2023-05-01", 0.01, "USD", "COMPLETED"),
    ])

    result = filter_invalid_amounts(orders)

    assert result["order_id"].tolist() == [101, 201]


# ---------------------------------------------------------------------------
# normalize_currency
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [("USD", "USD"), (" usd", "USD"), ("eur ", "EUR")])
def test_normalize_currency_trims_and_uppercases(raw, expected):
    assert normalize_currency(pd.Series([raw])).iloc[0] == expected


@pytest.mark.parametrize("raw", [None, "", "  "])
def test_normalize_currency_blank_becomes_missing(raw):
    assert pd.isna(normalize_currency(pd.Series([raw])).iloc[0])


# ---------------------------------------------------------------------------
# convert_to_usd
# ---------------------------------------------------------------------------

def test_convert_multiplies_by_rate_on_order_date():
    row = convert_single("2023-05-01", 200.00, "EUR")

    assert row["usd_amount"] == pytest.approx(220.00)
    assert row["exchange_rate"] == pytest.approx(1.10)
    assert row["fx_rate_source"] == FX_EXACT


def test_convert_small_unit_currency_is_multiplied_not_divided():
    row = convert_single("2023-05-01", 10000.00, "JPY")

    assert row["usd_amount"] == pytest.approx(70.00)


def test_convert_usd_order_is_unchanged():
    row = convert_single("2023-05-01", 150.00, "USD")

    assert row["usd_amount"] == pytest.approx(150.00)
    assert row["fx_rate_source"] == FX_NATIVE_USD


def test_convert_missing_currency_is_assumed_usd():
    row = convert_single("2023-05-01", 120.00, None)

    assert row["usd_amount"] == pytest.approx(120.00)
    assert row["fx_rate_source"] == FX_ASSUMED_USD


def test_convert_missing_rate_for_date_is_assumed_usd():
    row = convert_single("2023-05-07", 89.00, "EUR")

    assert row["usd_amount"] == pytest.approx(89.00)
    assert row["exchange_rate"] == pytest.approx(1.0)
    assert row["fx_rate_source"] == FX_ASSUMED_USD


def test_convert_normalizes_currency_before_matching():
    row = convert_single("2023-05-01", 100.00, " eur")

    assert row["usd_amount"] == pytest.approx(110.00)
    assert row["fx_rate_source"] == FX_EXACT


def test_convert_missing_order_date_never_matches_rate_with_missing_date():
    rates = pd.concat([RATES, make_rates([("EUR", 99.0, None)])], ignore_index=True)

    row = convert_single(None, 50.00, "EUR", rates=rates)

    assert row["usd_amount"] == pytest.approx(50.00)
    assert row["fx_rate_source"] == FX_ASSUMED_USD


def test_convert_keeps_every_order_exactly_once():
    orders = make_orders([
        (101, 1, "2023-05-01", 150.00, "USD", "COMPLETED"),
        (102, 2, "2023-05-01", 200.00, "EUR", "COMPLETED"),
        (110, 8, "2023-05-07", 89.00, "EUR", "COMPLETED"),
    ])

    result = convert_to_usd(orders, RATES)

    assert result["order_id"].tolist() == [101, 102, 110]


def test_convert_raises_on_duplicate_rates():
    rates = pd.concat([RATES, make_rates([("EUR", 1.20, "2023-05-01")])], ignore_index=True)

    with pytest.raises(pd.errors.MergeError):
        convert_single("2023-05-01", 200.00, "EUR", rates=rates)


# ---------------------------------------------------------------------------
# clean_orders (all rules together)
# ---------------------------------------------------------------------------

def test_clean_orders_filters_then_converts_and_keeps_context_columns():
    orders = make_orders([
        (102, 2, "2023-05-01", 200.00, "EUR", "COMPLETED"),
        (103, 3, "2023-05-01", -50.00, "USD", "SYSTEM_ERROR"),
        (106, 99, "2023-05-01", 75.50, "USD", "COMPLETED"),   # orphan customer
        (109, 7, "2023-05-01", 15.99, "USD", "CANCELLED"),
    ])

    result = clean_orders(orders, RATES).set_index("order_id")

    assert list(result.index) == [102, 106, 109]
    assert result.loc[102, "usd_amount"] == pytest.approx(220.00)
    assert result.loc[106, "customer_id"] == 99
    assert result.loc[109, "status"] == "CANCELLED"
