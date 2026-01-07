import os
import pandas as pd
from sqlalchemy import create_engine, text

SOURCE_DB_URL = os.environ.get("SOURCE_DB_URL")  # put in env, not in code

def extract_orders(since_ts: str) -> pd.DataFrame:
    engine = create_engine(SOURCE_DB_URL)
    q = text("""
        SELECT order_id, customer_id, order_total, order_status, created_at
        FROM orders
        WHERE created_at >= :since_ts
    """)
    return pd.read_sql(q, engine, params={"since_ts": since_ts})
