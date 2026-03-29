import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote_plus

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# --- What is create_engine? ---
# SQLAlchemy's create_engine creates a connection pool to MySQL
# Much faster than psycopg2 for loading large dataframes
# pandas .to_sql() method uses this engine to load data

def get_engine():
    user     = os.getenv("DB_USER")
    password = quote_plus(os.getenv("DB_PASSWORD"))  # encodes @ safely
    host     = os.getenv("DB_HOST")
    port     = os.getenv("DB_PORT")
    db       = os.getenv("DB_NAME")
    url      = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

# --- What is this URL format? ---
# mysql+mysqlconnector = use MySQL with mysql-connector-python library
# user:password = your credentials
# @host:port = where MySQL is running
# /db = which database to connect to

def clean_orders(df):
    df = df.dropna(subset=["order_id", "customer_id"])
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"], errors="coerce")
    df = df.drop_duplicates(subset=["order_id"])
    return df

def clean_order_items(df):
    df = df.dropna(subset=["order_id", "product_id"])
    df["price"] = df["price"].astype(float).round(2)
    df["freight_value"] = df["freight_value"].astype(float).round(2)
    df = df.drop_duplicates()
    return df

def clean_customers(df):
    df = df.dropna(subset=["customer_id"])
    df["customer_city"] = df["customer_city"].str.strip().str.title()
    df["customer_state"] = df["customer_state"].str.strip().str.upper()
    df = df.drop_duplicates(subset=["customer_id"])
    return df

def clean_products(df):
    df = df.dropna(subset=["product_id"])
    df["product_category_name"] = df["product_category_name"].fillna("unknown")
    df = df.drop_duplicates(subset=["product_id"])
    return df

def clean_payments(df):
    df = df.dropna(subset=["order_id"])
    df["payment_value"] = df["payment_value"].astype(float).round(2)
    df = df.drop_duplicates()
    return df

def load_table(df, table_name, engine):
    print(f"  Loading {table_name}... ({len(df)} rows)")
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )
    print(f"  ✅ {table_name} loaded successfully!")

# --- What is chunksize? ---
# Instead of loading all rows at once (slow, memory heavy)
# chunksize=1000 loads 1000 rows at a time
# This is BATCH PROCESSING — your resume claim!

def run_load():
    print("\n🚀 Starting Data Load...")
    print("=" * 50)

    engine = get_engine()
    base   = Path(__file__).parent / "data"

    # Load each table
    print("\n📂 Loading Orders...")
    orders = clean_orders(pd.read_csv(base / "olist_orders_dataset.csv"))
    load_table(orders, "orders", engine)

    print("\n📂 Loading Order Items...")
    items = clean_order_items(pd.read_csv(base / "olist_order_items_dataset.csv"))
    load_table(items, "order_items", engine)

    print("\n📂 Loading Customers...")
    customers = clean_customers(pd.read_csv(base / "olist_customers_dataset.csv"))
    load_table(customers, "customers", engine)

    print("\n📂 Loading Products...")
    products = clean_products(pd.read_csv(base / "olist_products_dataset.csv"))
    load_table(products, "products", engine)

    print("\n📂 Loading Sellers...")
    sellers = pd.read_csv(base / "olist_sellers_dataset.csv").drop_duplicates()
    load_table(sellers, "sellers", engine)

    print("\n📂 Loading Payments...")
    payments = clean_payments(pd.read_csv(base / "olist_order_payments_dataset.csv"))
    load_table(payments, "payments", engine)

    print("\n📂 Loading Category Translations...")
    category = pd.read_csv(base / "product_category_name_translation.csv")
    load_table(category, "category_translation", engine)

    print("\n" + "=" * 50)
    print("🎉 All tables loaded into MySQL successfully!")

if __name__ == "__main__":
    run_load()