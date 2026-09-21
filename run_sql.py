"""Run each SQL statement in a .sql file against a SQLite database and print the results.

Usage:
    python run_sql.py exploration.sql
    python run_sql.py clv_report.sql analytics.db
"""
import sqlite3
import sys
from contextlib import closing

import pandas as pd


def is_only_comments(chunk: str) -> bool:
    """Return True if a chunk contains nothing but SQL comments or blank lines."""
    lines = [line.strip() for line in chunk.splitlines()]
    return all(line == "" or line.startswith("--") for line in lines)


def run_sql_file(sql_path: str, db_path: str = "shopdata.db") -> None:
    with open(sql_path, encoding="utf-8") as f:
        script = f.read()

    statements = [s.strip() for s in script.split(";") if not is_only_comments(s)]

    # mode=ro opens the database read-only, so we can never modify the source data
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        for stmt in statements:
            print("=" * 80)
            print(stmt)
            print("-" * 80)
            df = pd.read_sql_query(stmt, conn)
            print(df.to_string(index=False))
            print()


if __name__ == "__main__":
    sql_file = sys.argv[1]
    db_file = sys.argv[2] if len(sys.argv) > 2 else "shopdata.db"
    run_sql_file(sql_file, db_file)