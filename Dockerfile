ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_MAJOR_MINOR=3.13

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && ln -sfn /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/default-java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

ENV AIRFLOW_HOME=/opt/airflow
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
