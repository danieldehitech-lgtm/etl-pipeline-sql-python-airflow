import os
import pandas as pd
from sqlalchemy import create_engine

TARGET_DB_URL = os.environ.get("TARGET_DB_URL")

def load_to_staging(df: pd.DataFrame) -> None:
    engine = create_engine(TARGET_DB_URL)
    df.to_sql("stg_orders", engine, if_exists="append", index=False)
