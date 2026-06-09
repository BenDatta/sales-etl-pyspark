from spark_session import get_spark_session
from pyspark.sql import functions as F


spark = get_spark_session("sales-etl-pyspark")

filepath = "data/sales.csv"

def ingest_sales_data(filepath):
    data = spark.read.csv(filepath, header=True)
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

    transformed_data =  (
        data
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("order_year", F.year("order_date"))
    )

    return transformed_data


if __name__ == "__main__":
    df = ingest_sales_data(filepath)
    data = transform_sales_data(df)

