import os
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String

SERVER = "sql-retail-dev-cin.database.windows.net"
DB = "oltp_retail"
USER = "sqladmin"
PWD = os.environ["SQL_ADMIN_PASSWORD"]  # export this in your shell, never hardcode

conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{SERVER},1433;Database={DB};Uid={USER};Pwd={PWD};"
    "Encrypt=yes;TrustServerCertificate=no;"
)
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
    fast_executemany=True,
)

# file -> (table, datetime columns)
FILES = {
    "olist_customers_dataset.csv": ("customers", []),
    "olist_orders_dataset.csv": ("orders", [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"]),
    "olist_order_items_dataset.csv": ("order_items", ["shipping_limit_date"]),
    "olist_order_payments_dataset.csv": ("order_payments", []),
    "olist_products_dataset.csv": ("products", []),
    "olist_sellers_dataset.csv": ("sellers", []),
    "product_category_name_translation.csv": ("product_category_translation", []),
}

with engine.begin() as conn:
    conn.execute(text(
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='sales') "
        "EXEC('CREATE SCHEMA sales')"))

for fname, (table, date_cols) in FILES.items():
    df = pd.read_csv(os.path.join("data", fname), parse_dates=date_cols)
    dtype = {c: String(255) for c in df.select_dtypes("object").columns}
    df.to_sql(table, engine, schema="sales", if_exists="replace",
              index=False, dtype=dtype, chunksize=5000)
    print(f"loaded {table}: {len(df):,} rows")