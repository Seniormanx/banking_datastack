FROM apache/airflow:2.9.3

USER airflow

RUN python -m pip install --no-cache-dir clickhouse-connect
