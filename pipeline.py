"""Prefect flow: extract raw ShopData views, clean them, and load them into analytics.db.

Run:
    python pipeline.py

The cleaning rules live in transforms.py as plain pandas functions. This module only
handles orchestration: reading, calling the transforms, logging, and writing.
"""
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from transforms import FX_ASSUMED_USD, UNKNOWN_EMAIL, clean_customers, clean_orders

SOURCE_VIEWS = ("vw_raw_customers", "vw_raw_orders", "vw_exchange_rates")

REQUIRED_COLUMNS = {
    "vw_raw_customers": ["customer_id", "full_name", "email", "phone", "signup_date"],
    "vw_raw_orders": ["order_id", "customer_id", "order_date", "total_amount", "currency", "status"],
    "vw_exchange_rates": ["currency", "rate_to_usd", "date"],
}

# Fallback outputs allowed by the assignment if analytics.db cannot be written
CSV_FALLBACK_NAMES = {"dim_customers": "clean_customers.csv", "fct_orders": "clean_orders.csv"}


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=5)
def extract_view(db_path: str, view_name: str) -> pd.DataFrame:
    """Read one source view from the SQLite database, opened read-only.

    Retries cover transient problems such as the database file being locked.
    """
    logger = get_run_logger()

    # The view name is inserted into SQL text, so only allow known names
    if view_name not in SOURCE_VIEWS:
        raise ValueError(f"Unknown source view: {view_name!r}")

    uri = f"{Path(db_path).resolve().as_uri()}?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)
    except sqlite3.Error:
        logger.exception("Failed to read view %s from %s", view_name, db_path)
        raise

    missing = set(REQUIRED_COLUMNS[view_name]) - set(df.columns)
    if missing:
        raise ValueError(f"{view_name} is missing expected columns: {sorted(missing)}")

    logger.info("Extracted %d rows from %s", len(df), view_name)
    return df


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

@task
def transform_customers(raw_customers: pd.DataFrame) -> pd.DataFrame:
    """Clean customers and log what the cleaning rules changed."""
    logger = get_run_logger()
    try:
        customers = clean_customers(raw_customers)
    except Exception:
        logger.exception("Customer transformation failed")
        raise

    logger.info(
        "Customers: %d raw rows -> %d unique customers (%d duplicates removed)",
        len(raw_customers), len(customers), len(raw_customers) - len(customers),
    )
    logger.info(
        "Customers with placeholder email: %d, without a phone number: %d",
        (customers["email"] == UNKNOWN_EMAIL).sum(), customers["phone"].isna().sum(),
    )
    return customers


@task
def transform_orders(raw_orders: pd.DataFrame, raw_rates: pd.DataFrame) -> pd.DataFrame:
    """Clean orders, convert amounts to USD, and warn about unreliable conversions."""
    logger = get_run_logger()
    try:
        orders = clean_orders(raw_orders, raw_rates)
    except Exception:
        logger.exception("Order transformation failed")
        raise

    logger.info(
        "Orders: %d raw rows -> %d valid (%d removed with zero, negative or missing amount)",
        len(raw_orders), len(orders), len(raw_orders) - len(orders),
    )
    logger.info("USD conversion sources: %s", orders["fx_rate_source"].value_counts().to_dict())

    assumed = orders.loc[(orders["fx_rate_source"] == FX_ASSUMED_USD) & orders["currency"].notna()]
    if not assumed.empty:
        logger.warning(
            "%d non-USD orders had no exchange rate for their date and were assumed to be USD: %s",
            len(assumed), assumed["order_id"].tolist(),
        )
    return orders


@task
def check_orphan_orders(orders: pd.DataFrame, customers: pd.DataFrame) -> int:
    """Warn about orders whose customer_id is not in the customer dimension."""
    logger = get_run_logger()
    orphans = orders.loc[~orders["customer_id"].isin(customers["customer_id"])]
    if orphans.empty:
        logger.info("All orders reference a known customer")
    else:
        logger.warning(
            "%d orders reference unknown customers (kept in fct_orders, excluded from CLV): %s",
            len(orphans), orphans["order_id"].tolist(),
        )
    return len(orphans)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

@task(cache_policy=NO_CACHE)
def load_table(
    df: pd.DataFrame,
    table_name: str,
    db_path: str,
    indexes: dict[str, bool] | None = None,
) -> str:
    """Write a DataFrame to a SQLite table, replacing it if it exists.

    indexes maps column name -> whether the index is UNIQUE.
    If the database cannot be written, falls back to a CSV file and returns its path.
    """
    logger = get_run_logger()
    try:
        with closing(sqlite3.connect(db_path)) as conn:
            with conn:  # commit on success, roll back on error
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                for column, unique in (indexes or {}).items():
                    kind = "UNIQUE INDEX" if unique else "INDEX"
                    conn.execute(
                        f"CREATE {kind} IF NOT EXISTS idx_{table_name}_{column} "
                        f"ON {table_name} ({column})"
                    )
    except sqlite3.Error:
        csv_path = CSV_FALLBACK_NAMES[table_name]
        logger.exception("Could not write %s to %s, falling back to %s", table_name, db_path, csv_path)
        df.to_csv(csv_path, index=False)
        return csv_path

    logger.info("Loaded %d rows into %s (%s)", len(df), table_name, db_path)
    return db_path


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="shopdata-etl")
def shopdata_etl(source_db: str = "shopdata.db", target_db: str = "analytics.db") -> dict[str, int]:
    """Extract raw views, clean them, and load dim_customers and fct_orders."""
    logger = get_run_logger()

    # Fail fast: retrying cannot fix a missing file
    if not Path(source_db).is_file():
        raise FileNotFoundError(f"Source database not found: {source_db}")

    raw_customers = extract_view(source_db, "vw_raw_customers")
    raw_orders = extract_view(source_db, "vw_raw_orders")
    raw_rates = extract_view(source_db, "vw_exchange_rates")

    customers = transform_customers(raw_customers)
    orders = transform_orders(raw_orders, raw_rates)
    check_orphan_orders(orders, customers)

    load_table(customers, "dim_customers", target_db, indexes={"customer_id": True})
    load_table(orders, "fct_orders", target_db, indexes={"order_id": True, "customer_id": False})

    summary = {"dim_customers": len(customers), "fct_orders": len(orders)}
    logger.info("Pipeline finished: %s", summary)
    return summary


if __name__ == "__main__":
    shopdata_etl()
