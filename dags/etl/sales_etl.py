from spark_session import get_spark_session
from pyspark.sql import functions as F
import pandas as pd

filepath = "/opt/airflow/data/sales.csv"
STAGING_PATH = "/opt/airflow/data/staging/ingested"

_spark = None


def _get_spark():
    global _spark
    if _spark is None:
        _spark = get_spark_session("sales-etl-pyspark")
    return _spark


def ingest_sales_data(filepath=filepath):
    data = _get_spark().read.csv(filepath, header=True)
    print("Total Row Ingested:", data.count())
    return data


def transform_sales_data(data):
    cast_map = {
        "Quantity": "int",
        "SalesOrderLineNumber": "int",
        "OrderDate": "date",
        "UnitPrice": "float",
        "TaxAmount": "float",
    }
    for col_name, dtype in cast_map.items():
        data = data.withColumn(col_name, F.col(col_name).cast(dtype))

    rename_map = {
        "SalesOrderNumber": "sales_order_number",
        "SalesOrderLineNumber": "sales_order_line_number",
        "OrderDate": "order_date",
        "CustomerName": "customer_name",
        "EmailAddress": "email_address",
        "Item": "item",
        "Quantity": "quantity",
        "UnitPrice": "unit_price",
        "TaxAmount": "tax_amount",
    }
    for old_name, new_name in rename_map.items():
        data = data.withColumnRenamed(old_name, new_name)

    transformed_data = (
        data.withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("order_year", F.year("order_date"))
    )

    return transformed_data


def run_ingest():
    df = ingest_sales_data(filepath)
    df.write.mode("overwrite").parquet(STAGING_PATH)


def run_transform():
    data = _get_spark().read.parquet(STAGING_PATH)
    transformed = transform_sales_data(data)
    print("Transformed rows:", transformed.count())


if __name__ == "__main__":
    df = ingest_sales_data(filepath)
    data = transform_sales_data(df)
