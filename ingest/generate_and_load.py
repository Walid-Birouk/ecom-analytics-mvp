import os
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text


# ------------------------------------------------------------------------------
# Config / Helpers
# ------------------------------------------------------------------------------

def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def make_engine():
    user = env("POSTGRES_USER", "postgres")
    pwd = env("POSTGRES_PASSWORD", "postgres")
    db = env("POSTGRES_DB", "warehouse")
    host = env("POSTGRES_HOST", "localhost")
    port = env("POSTGRES_PORT", "5432")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)


def ensure_schema(engine, schema: str):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))


# ------------------------------------------------------------------------------
# Data generation
# ------------------------------------------------------------------------------

def generate_data(seed: int = 42, n_customers: int = 5000, n_products: int = 800, n_orders: int = 20000):
    random.seed(seed)
    np.random.seed(seed)
    fake = Faker()
    Faker.seed(seed)

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=365)

    country_city_map = {
        "FR": ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"],
        "BE": ["Bruxelles", "Anvers", "Gand", "Liège", "Charleroi"],
        "DZ": ["Alger", "Oran", "Constantine", "Annaba", "Sétif"],
        "DE": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"],
        "NL": ["Amsterdam", "Rotterdam", "Utrecht", "Eindhoven", "The Hague"],
        "ES": ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"],
        "IT": ["Milan", "Rome", "Turin", "Naples", "Bologna"],
    }
    countries = list(country_city_map.keys())

    # Customers
    customers = []
    for i in range(n_customers):
        country = random.choice(countries)
        city = random.choice(country_city_map[country])
        created_at = start + timedelta(days=random.randint(0, 364), seconds=random.randint(0, 86400))

        customers.append(
            {
                "customer_id": f"C{i:06d}",
                "created_at": created_at.isoformat(),
                "country": country,
                "city": city,
            }
        )
    customers_df = pd.DataFrame(customers)

    # Products
    categories = ["electronics", "fashion", "home", "beauty", "sports", "books", "grocery"]
    products = []
    for i in range(n_products):
        base_price = round(max(5, np.random.lognormal(mean=3.2, sigma=0.5)), 2)
        products.append(
            {
                "product_id": f"P{i:06d}",
                "category": random.choice(categories),
                "price": float(base_price),
            }
        )
    products_df = pd.DataFrame(products)

    # Orders / Items / Payments
    statuses = ["paid", "shipped", "delivered", "cancelled"]
    status_probs = [0.35, 0.25, 0.30, 0.10]

    orders = []
    order_items = []
    payments = []

    # More orders near end of period (light seasonality)
    order_days = np.random.triangular(left=0, mode=320, right=364, size=n_orders).astype(int)

    for i in range(n_orders):
        order_id = f"O{i:07d}"
        cust = customers_df.sample(1).iloc[0]["customer_id"]
        day_offset = int(order_days[i])
        order_ts = start + timedelta(days=day_offset, seconds=random.randint(0, 86400))
        status = np.random.choice(statuses, p=status_probs)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": cust,
                "order_ts": order_ts.isoformat(),
                "status": status,
            }
        )

        # 1 to 5 items
        n_items = random.randint(1, 5)
        chosen = products_df.sample(n_items)

        total_amount = 0.0
        for _, p in chosen.iterrows():
            qty = random.randint(1, 3)
            unit_price = float(p["price"])
            total_amount += qty * unit_price

            order_items.append(
                {
                    "order_id": order_id,
                    "product_id": p["product_id"],
                    "quantity": qty,
                    "unit_price": unit_price,
                }
            )

        # Payments only if not cancelled
        if status != "cancelled":
            method = np.random.choice(["card", "paypal", "bank_transfer"], p=[0.75, 0.20, 0.05])
            paid_ts = order_ts + timedelta(minutes=random.randint(1, 180))

            payments.append(
                {
                    "order_id": order_id,
                    "payment_method": str(method),
                    "amount": round(total_amount, 2),
                    "paid_ts": paid_ts.isoformat(),
                }
            )

    orders_df = pd.DataFrame(orders)
    order_items_df = pd.DataFrame(order_items)
    payments_df = pd.DataFrame(payments)

    return customers_df, products_df, orders_df, order_items_df, payments_df


# ------------------------------------------------------------------------------
# IO: CSV + Postgres load
# ------------------------------------------------------------------------------

def write_csvs(out_dir: str, dfs):
    os.makedirs(out_dir, exist_ok=True)
    names = ["customers", "products", "orders", "order_items", "payments"]
    for name, df in zip(names, dfs):
        df.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)


def _ensure_table_exists(conn, df: pd.DataFrame, schema: str, table: str):
    # Creates the table with the right columns if it doesn't exist (0 rows).
    df.head(0).to_sql(table, conn, schema=schema, if_exists="append", index=False)


def _replace_table_data(conn, df: pd.DataFrame, schema: str, table: str):
    # TRUNCATE keeps the table object (and dependent views) intact.
    full = f"{schema}.{table}"
    conn.execute(text(f"TRUNCATE TABLE {full};"))
    df.to_sql(table, conn, schema=schema, if_exists="append", index=False)


def load_to_postgres(engine, out_dir: str):
    schema = "raw"
    mapping = {
        "customers": "raw_customers",
        "products": "raw_products",
        "orders": "raw_orders",
        "order_items": "raw_order_items",
        "payments": "raw_payments",
    }

    with engine.begin() as conn:
        for csv_name, table in mapping.items():
            path = os.path.join(out_dir, f"{csv_name}.csv")
            df = pd.read_csv(path)

            _ensure_table_exists(conn, df, schema=schema, table=table)
            _replace_table_data(conn, df, schema=schema, table=table)

        # Helpful indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_orders_customer ON raw.raw_orders(customer_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_items_order ON raw.raw_order_items(order_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_payments_order ON raw.raw_payments(order_id);"))


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    engine = make_engine()
    ensure_schema(engine, "raw")

    out_dir = os.path.join(os.path.dirname(__file__), "data")
    dfs = generate_data(seed=42, n_customers=5000, n_products=800, n_orders=20000)

    write_csvs(out_dir, dfs)
    load_to_postgres(engine, out_dir)

    print("Generated CSVs and loaded into Postgres (schema: raw).")
    print("Tables: raw.raw_customers, raw.raw_products, raw.raw_orders, raw.raw_order_items, raw.raw_payments")


if __name__ == "__main__":
    main()
