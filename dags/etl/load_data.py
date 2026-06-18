import os
import psycopg2
from psycopg2.extras import execute_values
from spark_session import get_spark_session

SCHEMA = "sales_db"
TABLE = "sales"
FULL_TABLE = f'"{SCHEMA}"."{TABLE}"'
STAGING_PATH = "/opt/airflow/data/staging/transformed"
PRIMARY_KEYS = ["sales_order_number", "sales_order_line_number"]


def load_data_postgres():

    spark = get_spark_session("sales-etl-load")
    df = spark.read.parquet(STAGING_PATH)
    print("Rows to load:", df.count())
    pdf = df.toPandas()

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_CONN_HOST"],
        port=os.environ["POSTGRES_CONN_PORT"],
        dbname=os.environ["ELT_DATABASE_NAME"],
        user=os.environ["ELT_DATABASE_USERNAME"],
        password=os.environ["ELT_DATABASE_PASSWORD"],
    )

    cols = list(pdf.columns)
    non_pk_cols = [c for c in cols if c not in PRIMARY_KEYS]
    update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols)

    query = f"""
        INSERT INTO {FULL_TABLE} ({", ".join(f'"{c}"' for c in cols)})
        VALUES %s
        ON CONFLICT (sales_order_number, sales_order_line_number)
        DO UPDATE SET {update_clause}
    """

    with conn.cursor() as cur:
        execute_values(cur, query, pdf.values.tolist())

    conn.commit()
    conn.close()

    print(f"Loaded {len(pdf)} rows into {FULL_TABLE}")
