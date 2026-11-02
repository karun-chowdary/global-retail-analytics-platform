#!/usr/bin/env python3
"""Load the Olist CSV files into your local source databases.

  Oracle      -> orders, order_items, order_payments   (transactional data)
  SQL Server  -> customers, sellers, products, geolocation (master data)

Credentials come from environment variables or a .env file (never hard-code them):
  ORACLE_HOST, ORACLE_PORT (default 1521), ORACLE_SERVICE (e.g. FREEPDB1),
  ORACLE_USER, ORACLE_PASSWORD
  SQLSERVER_HOST, SQLSERVER_DATABASE (default RetailSource),
  SQLSERVER_USER, SQLSERVER_PASSWORD  (leave both empty to use Windows login)

Examples:
  python sources/load_sources.py --target all --data-dir data/raw
  # simulate history first, then new days arriving later (for incremental loads):
  python sources/load_sources.py --target oracle --to-date 2018-06-30
  python sources/load_sources.py --target oracle --mode append --from-date 2018-07-01 --to-date 2018-07-07
"""
import argparse
import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

import numpy as np
import pandas as pd

BATCH = 5000

ORACLE_TABLES = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "columns": ["order_id", "customer_id", "order_status",
                    "order_purchase_timestamp", "order_approved_at",
                    "order_delivered_carrier_date", "order_delivered_customer_date",
                    "order_estimated_delivery_date"],
        "timestamps": ["order_purchase_timestamp", "order_approved_at",
                       "order_delivered_carrier_date", "order_delivered_customer_date",
                       "order_estimated_delivery_date"],
        "ints": [],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "columns": ["order_id", "order_item_id", "product_id", "seller_id",
                    "shipping_limit_date", "price", "freight_value"],
        "timestamps": ["shipping_limit_date"],
        "ints": ["order_item_id"],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "columns": ["order_id", "payment_sequential", "payment_type",
                    "payment_installments", "payment_value"],
        "timestamps": [],
        "ints": ["payment_sequential", "payment_installments"],
    },
}

SQLSERVER_TABLES = {
    "customers": {
        "file": "olist_customers_dataset.csv",
        "columns": ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                    "customer_city", "customer_state"],
        "timestamps": [],
        "ints": ["customer_zip_code_prefix"],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "columns": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        "timestamps": [],
        "ints": ["seller_zip_code_prefix"],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "columns": ["product_id", "product_category_name", "product_name_lenght",
                    "product_description_lenght", "product_photos_qty",
                    "product_weight_g", "product_length_cm", "product_height_cm",
                    "product_width_cm"],
        "timestamps": [],
        "ints": ["product_name_lenght", "product_description_lenght",
                 "product_photos_qty", "product_weight_g", "product_length_cm",
                 "product_height_cm", "product_width_cm"],
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "columns": ["geolocation_zip_code_prefix", "geolocation_lat",
                    "geolocation_lng", "geolocation_city", "geolocation_state"],
        "timestamps": [],
        "ints": ["geolocation_zip_code_prefix"],
    },
}


def need(name, default=None):
    value = os.environ.get(name, default)
    if value is None or value == "":
        sys.exit(f"Missing setting {name}. Add it to your .env file (see .env.example).")
    return value


def read_table(data_dir, cfg):
    path = Path(data_dir) / cfg["file"]
    if not path.exists():
        sys.exit(f"File not found: {path}. Download the Olist dataset into {data_dir}.")
    df = pd.read_csv(path, usecols=cfg["columns"], encoding="utf-8")[cfg["columns"]]
    for col in cfg["timestamps"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in cfg["ints"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def to_native(value):
    """Convert pandas/numpy values into plain Python values (NaN/NaT/NA become None)."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        return value.item()
    return value


def prep_rows(df):
    return [[to_native(v) for v in row] for row in df.astype(object).itertuples(index=False, name=None)]


def filter_orders(orders, date_from, date_to):
    if date_from:
        orders = orders[orders["order_purchase_timestamp"] >= pd.Timestamp(date_from)]
    if date_to:
        orders = orders[orders["order_purchase_timestamp"] < pd.Timestamp(date_to) + pd.Timedelta(days=1)]
    return orders


def load_oracle(data_dir, mode, date_from, date_to):
    import oracledb

    dsn = f"{need('ORACLE_HOST', 'localhost')}:{need('ORACLE_PORT', '1521')}/{need('ORACLE_SERVICE', 'FREEPDB1')}"
    conn = oracledb.connect(user=need("ORACLE_USER"), password=need("ORACLE_PASSWORD"), dsn=dsn)
    cur = conn.cursor()
    print(f"Connected to Oracle at {dsn}")

    orders = filter_orders(read_table(data_dir, ORACLE_TABLES["orders"]), date_from, date_to)
    keep_ids = set(orders["order_id"])
    frames = {"orders": orders}
    for name in ("order_items", "order_payments"):
        df = read_table(data_dir, ORACLE_TABLES[name])
        frames[name] = df[df["order_id"].isin(keep_ids)]

    if mode == "truncate":
        for name in ("order_payments", "order_items", "orders"):
            cur.execute(f"TRUNCATE TABLE {name}")

    for name in ("orders", "order_items", "order_payments"):
        df = frames[name]
        cols = ORACLE_TABLES[name]["columns"]
        sql = f"INSERT INTO {name} ({', '.join(cols)}) VALUES ({', '.join(f':{i + 1}' for i in range(len(cols)))})"
        rows = prep_rows(df)
        for start in range(0, len(rows), BATCH):
            cur.executemany(sql, rows[start:start + BATCH])
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {name}")
        in_db = cur.fetchone()[0]
        print(f"  oracle.{name}: inserted {len(rows):,} rows, table now has {in_db:,}")
    conn.close()


def load_sqlserver(data_dir, mode):
    from sqlalchemy import create_engine, text

    host = need("SQLSERVER_HOST", "localhost")
    database = os.environ.get("SQLSERVER_DATABASE") or "RetailSource"
    user = os.environ.get("SQLSERVER_USER", "")
    password = os.environ.get("SQLSERVER_PASSWORD", "")
    driver = quote_plus("ODBC Driver 18 for SQL Server")
    if user:
        url = (f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(password)}@{host}/{database}"
               f"?driver={driver}&TrustServerCertificate=yes")
    else:
        url = (f"mssql+pyodbc://@{host}/{database}"
               f"?driver={driver}&trusted_connection=yes&TrustServerCertificate=yes")
    engine = create_engine(url, fast_executemany=True)
    print(f"Connected to SQL Server {host}/{database}")

    for name, cfg in SQLSERVER_TABLES.items():
        df = read_table(data_dir, cfg)
        df = df.astype(object).where(df.notna(), None)
        if mode == "truncate":
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE dbo.{name}"))
        df.to_sql(name, engine, schema="dbo", if_exists="append", index=False, chunksize=BATCH)
        with engine.connect() as conn:
            in_db = conn.execute(text(f"SELECT COUNT(*) FROM dbo.{name}")).scalar()
        print(f"  sqlserver.{name}: inserted {len(df):,} rows, table now has {in_db:,}")


def main():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="Load Olist CSVs into Oracle and SQL Server.")
    parser.add_argument("--target", choices=["oracle", "sqlserver", "all"], default="all")
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--mode", choices=["truncate", "append"], default="truncate",
                        help="truncate = empty tables first (default); append = add rows")
    parser.add_argument("--from-date", help="Oracle only: first order purchase date, YYYY-MM-DD")
    parser.add_argument("--to-date", help="Oracle only: last order purchase date, YYYY-MM-DD")
    args = parser.parse_args()

    if args.target in ("oracle", "all"):
        load_oracle(args.data_dir, args.mode, args.from_date, args.to_date)
    if args.target in ("sqlserver", "all"):
        load_sqlserver(args.data_dir, args.mode)
    print("Done. Compare the table counts above with the CSV row counts.")


if __name__ == "__main__":
    main()
