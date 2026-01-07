import pandas as pd

def run_quality_checks(df: pd.DataFrame) -> None:
    # basic, realistic checks
    if df.empty:
        raise ValueError("No rows after transformation")

    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order_id detected")

    if (df["order_total"] < 0).any():
        raise ValueError("Negative order_total detected")

    if df.isnull().any().any():
        raise ValueError("Null values detected")
