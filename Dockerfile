ARG AIRFLOW_VERSION=3.2.2

ARG PYTHON_MAJOR_MINOR=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR}

ENV AIRFLOW_HOME=/opt/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt
