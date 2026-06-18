# Sales ETL Pipeline — PySpark + Airflow
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

End-to-end sales data pipeline built with PySpark, Apache Airflow, and PostgreSQL, fully containerised with Docker Compose.

---

## What was built

Raw sales CSV data is ingested, cleaned, and loaded into a PostgreSQL database through a three-task Airflow DAG: **extract → transform → load**.

**Extract** — PySpark reads raw CSV files from `/data/raw/` and writes them to a Parquet staging area. Parquet was chosen for its columnar format, schema enforcement, and efficiency for downstream transforms.

**Transform** — PySpark applies schema enforcement, type casting, and column normalisation. Output lands in `/data/staging/transformed/` as clean Parquet, fully decoupled from the extract step so either can be re-run independently.

**Load** — 32,718 rows are upserted into `sales_db.sales` in PostgreSQL using `psycopg2`. The upsert strategy (`ON CONFLICT (sales_order_number, sales_order_line_number) DO UPDATE SET ...`) makes every run idempotent — safe to re-trigger without duplicates or failures.

---

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.x — CeleryExecutor |
| Processing | PySpark |
| Storage | PostgreSQL |
| Containerisation | Docker Compose |
| Message Broker | Redis |
| Analysis | Jupyter Notebook (`sales_analysis_pyspark.ipynb`) |
| Language | Python 3.13 |

---

## Project structure

```
sales-etl-pyspark/
├── dags/
│   ├── main.py                     # DAG definition
│   └── etl/
│       ├── extract_data.py
│       ├── transform_data.py
│       └── load_data_postgres.py
├── data/
│   ├── raw/
│   └── staging/transformed/
├── spark_session.py
├── sales_analysis_pyspark.ipynb
├── Dockerfile
├── docker-compose.yaml
└── requirements.txt
```

---

## Key design decisions

**PySpark for ~32K rows** — designed for scalability. The same pipeline handles millions of rows without code changes by adjusting the Spark cluster config.

**Parquet staging layer** — decouples extract and transform. Either step can be re-run without re-processing the other.

**Upsert over truncate-and-load** — no data loss on partial failures. Re-triggering the DAG always produces the correct final state.

**Environment via Docker** — all credentials injected through `docker-compose --env-file .env`, keeping secrets out of the codebase entirely.

---
