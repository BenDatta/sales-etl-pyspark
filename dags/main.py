from datetime import datetime, timedelta
from airflow import DAG  # type:ignore
from airflow.operators.python import PythonOperator  # type:ignore

from etl.sales_etl import run_ingest, run_transform
from etl.load_data_postgres import load_data_postgres

default_args = {
    "owner": "data_engineering_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="sales_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest",
        python_callable=run_ingest,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data_postgres,
    )

    ingest >> transform >> load
