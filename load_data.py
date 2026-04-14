"""
load_data.py
------------
Downloads Online Retail II dataset from the UCI URL,
cleans it, and then loads it into a PostgreSQL database.

Requirements:
    pip install pandas openpyxl sqlalchemy psycopg2-binary
"""

import pandas as pd
from sqlalchemy import create_engine, text
import os

# ── CONFIG ────────────────────────────────────────────────────────────────────
DATA_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "database": os.getenv("DB_NAME",     "retail_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "Jaymcbrown4444"),   
}

# ── LOAD ──────────────────────────────────────────────────────────────────────
def load_raw_data(url: str) -> pd.DataFrame:
    print(f"Downloading dataset from:\n  {url}\n")
    df_2009 = pd.read_excel(url, sheet_name="Year 2009-2010", engine="openpyxl")
    df_2010 = pd.read_excel(url, sheet_name="Year 2010-2011", engine="openpyxl")
    df = pd.concat([df_2009, df_2010], ignore_index=True)
    print(f"Raw records loaded: {len(df):,}")
    return df


# ── CLEAN ─────────────────────────────────────────────────────────────────────
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("\nCleaning data...")

    # Standardise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Drop rows missing CustomerID
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    print(f"  Dropped {before - len(df):,} rows with missing CustomerID")

    # Remove cancellations (Invoice starting with C)
    before = len(df)
    df = df[~df["invoice"].astype(str).str.startswith("C")]
    print(f"  Dropped {before - len(df):,} cancellation records")

    # Remove negative or zero quantities and prices
    before = len(df)
    df = df[(df["quantity"] > 0) & (df["price"] > 0)]
    print(f"  Dropped {before - len(df):,} rows with invalid quantity/price")

    # Cast types
    df["customer_id"]   = df["customer_id"].astype(int)
    df["invoicedate"]  = pd.to_datetime(df["invoicedate"]) 
    df["revenue"]       = df["quantity"] * df["price"]

    print(f"\nValidated records: {len(df):,}")
    return df


# ── SCHEMA ────────────────────────────────────────────────────────────────────
CREATE_SCHEMA = """
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS customers   CASCADE;

CREATE TABLE transactions (
    id           SERIAL PRIMARY KEY,
    invoice      VARCHAR(20)   NOT NULL,
    stockcode    VARCHAR(20)   NOT NULL,
    description  TEXT,
    quantity     INTEGER       NOT NULL,
    invoicedate  TIMESTAMP     NOT NULL,
    price        NUMERIC(10,2) NOT NULL,
    customer_id  INTEGER       NOT NULL,
    country      VARCHAR(60),
    revenue      NUMERIC(12,2) NOT NULL
);

CREATE INDEX idx_transactions_customer  ON transactions(customer_id);
CREATE INDEX idx_transactions_date      ON transactions(invoicedate);
CREATE INDEX idx_transactions_invoice   ON transactions(invoice);
"""


# ── LOAD TO POSTGRES ──────────────────────────────────────────────────────────
def load_to_postgres(df: pd.DataFrame, config: dict) -> None:
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)

    print("\nCreating schema...")
    with engine.connect() as conn:
        conn.execute(text(CREATE_SCHEMA))
        conn.commit()

    print("Loading data into PostgreSQL...")
    df.to_sql("transactions", engine, if_exists="append", index=False,
                 chunksize=5000, method="multi")

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar()
    print(f"\nDone. {count:,} rows loaded into 'transactions' table.")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df_raw   = load_raw_data(DATA_URL)
    df_clean = clean_data(df_raw)
    load_to_postgres(df_clean, DB_CONFIG)
