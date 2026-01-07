from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.extract.extract_orders import extract_orders
from src.transform.transform_orders import transform_orders
from src.quality.checks import run_quality_checks
from src.load.load_staging import load_to_staging
from src.load.build_daily_sales import build_daily_sales

_state = {}

def task_extract():
    df = extract_orders(since_ts="2024-01-01")
    _state["orders"] = df

def task_transform_validate_load():
    df = _state["orders"]
    df2 = transform_orders(df)
    run_quality_checks(df2)
    load_to_staging(df2)

def task_mart():
    build_daily_sales()

with DAG(
    dag_id="daily_orders_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=task_extract)
    t2 = PythonOperator(task_id="transform_validate_load", python_callable=task_transform_validate_load)
    t3 = PythonOperator(task_id="build_daily_sales_mart", python_callable=task_mart)

    t1 >> t2 >> t3
