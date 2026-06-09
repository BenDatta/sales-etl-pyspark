from spark_session import get_spark_session

spark = get_spark_session("sales-etl-pyspark")


def ingest_sales_data(filepath):
    data = spark.read.csv(filepath, header=True)
    print("Total Row Count:", data.count())
    return data



if __name__ == "__main__":
    df = ingest_sales_data("data/sales.csv")
