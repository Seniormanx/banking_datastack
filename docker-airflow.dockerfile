
FROM apache/airflow:2.11.2-python3.10

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==2.11.2" \
    -r /requirements.txt \
    dbt-clickhouse \
    clickhouse-connect

USER airflow