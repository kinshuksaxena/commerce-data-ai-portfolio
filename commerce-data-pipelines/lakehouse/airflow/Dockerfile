FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.16 \
    fastavro==1.9.4 \
    pyiceberg==0.6.1 \
    pyarrow==16.0.0 \
    dbt-core==1.7.14 \
    dbt-clickhouse==1.7.3 \
    psycopg2-binary==2.9.9 \
    google-cloud-storage==2.16.0 \
    google-cloud-bigquery==3.25.0 \
    requests==2.32.3
