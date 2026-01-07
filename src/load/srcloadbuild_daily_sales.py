import os
from sqlalchemy import create_engine, text

TARGET_DB_URL = os.environ.get("TARGET_DB_URL")

def build_daily_sales() -> None:
    engine = create_engine(TARGET_DB_URL)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE mart_daily_sales;"))
        conn.execute(text("""
            INSERT INTO mart_daily_sales (sales_date, total_orders, total_revenue)
            SELECT
              DATE(created_at) AS sales_date,
              COUNT(*) AS total_orders,
              SUM(order_total) AS total_revenue
            FROM stg_orders
            GROUP BY DATE(created_at)
        """))
