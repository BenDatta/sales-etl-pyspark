from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

from etl.sales_etl import run_ingest, run_transform

default_args = {
    "owner": "data_engineering_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="sales_etl",
    start_date=datetime(2020, 10, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    tags=["engineering"],
) as dag:
    ingest_data = PythonOperator(
        task_id="ingest_sales_data",
        python_callable=run_ingest,
    )

    transform_data = PythonOperator(
        task_id="transform_sales_data",
        python_callable=run_transform,
    )

    ingest_data >> transform_data
