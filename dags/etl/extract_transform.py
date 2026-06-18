from spark_session import get_spark_session
from pyspark.sql import functions as F
import os

filepath = "/opt/airflow/data/sales.csv"

INGEST_PATH = "/opt/airflow/data/staging/ingested"
TRANSFORM_PATH = "/opt/airflow/data/staging/transformed"

_spark = None


def get_spark():
    global _spark
    if _spark is None:
        _spark = get_spark_session("sales-etl-pyspark")
    return _spark


def run_ingest():
    spark = get_spark()

    df = spark.read.csv(filepath, header=True, inferSchema=True)

    print("Ingested rows:", df.count())

    # ensure folder exists
    os.makedirs(INGEST_PATH, exist_ok=True)

    df.write.mode("overwrite").parquet(INGEST_PATH)

    print("Ingest written to:", INGEST_PATH)


def run_transform():
    spark = get_spark()

    # safety check (VERY IMPORTANT in Airflow)
    if not os.path.exists(INGEST_PATH):
        raise ValueError(f"Missing ingest path: {INGEST_PATH}")

    df = spark.read.parquet(INGEST_PATH)

    print("Transform input rows:", df.count())

    cast_map = {
        "Quantity": "int",
        "SalesOrderLineNumber": "int",
        "OrderDate": "date",
        "UnitPrice": "float",
        "TaxAmount": "float",
    }

    for c, t in cast_map.items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))

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

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    df = (
        df.withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("order_year", F.year("order_date"))
    )

    os.makedirs(TRANSFORM_PATH, exist_ok=True)

    df.write.mode("overwrite").parquet(TRANSFORM_PATH)

    print("Transformed rows:", df.count())
