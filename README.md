# Sales ETL Pipeline
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

End-to-end sales data pipeline built with PySpark, Apache Airflow, and PostgreSQL, fully containerised with Docker Compose.

---

## Data Pipeline

![Pipeline](./sales_etl.png)

---

## ETL Workflow

### Extract
- Reads raw sales CSV data using PySpark.
- Stores data in a Parquet staging layer for efficient downstream processing.
- Ingests 32,718 sales records.

### Transform
PySpark applies:
- Data type enforcement and casting:
  - `Quantity` → Integer
  - `SalesOrderLineNumber` → Integer
  - `OrderDate` → Date
  - `UnitPrice` → Float
  - `TaxAmount` → Float
- Column standardisation (`CamelCase` → `snake_case`).
- Date enrichment: `order_year`, `order_month`, `order_day`.
- Writes the cleaned dataset back to a transformed Parquet layer.

### Load
- Reads transformed Parquet data.
- Loads records into PostgreSQL (`sales_db.sales`).
- Uses an UPSERT strategy with `ON CONFLICT` to ensure idempotent loads and prevent duplicate records.

---

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.2.2|
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
│       ├── extract_transform.py
│       └── load_data.py
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
