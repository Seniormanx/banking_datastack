from datetime import datetime, timezone
import io
import os

from airflow import DAG
from airflow.operators.python import PythonOperator


MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]

CLICKHOUSE_HOST = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_PORT = int(os.environ["CLICKHOUSE_PORT"])
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_DB = os.environ["CLICKHOUSE_DB"]


def get_minio_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
    )


def get_clickhouse_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def ensure_control_table(client):
    client.command("""
        CREATE TABLE IF NOT EXISTS loaded_objects (
            object_key String,
            loaded_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree()
        ORDER BY object_key
    """)


def get_loaded_objects(client):
    rows = client.query(
        "SELECT object_key FROM loaded_objects"
    ).result_rows

    return {row[0] for row in rows}


def load_customers(client, df):
    import pandas as pd

    df["created_at"] = pd.to_datetime(
        df["created_at"],
        utc=True,
    )

    rows = [
        (
            int(row.id),
            str(row.first_name),
            str(row.last_name),
            str(row.email),
            row.created_at.to_pydatetime(),
        )
        for row in df.itertuples(index=False)
    ]

    client.insert(
        "customers",
        rows,
        column_names=[
            "id",
            "first_name",
            "last_name",
            "email",
            "created_at",
        ],
    )


def load_accounts(client, df):
    import pandas as pd

    df["balance"] = pd.to_numeric(df["balance"])

    df["created_at"] = pd.to_datetime(
        df["created_at"],
        utc=True,
    )

    rows = [
        (
            int(row.id),
            int(row.customer_id),
            str(row.account_type),
            float(row.balance),
            str(row.currency),
            row.created_at.to_pydatetime(),
        )
        for row in df.itertuples(index=False)
    ]

    client.insert(
        "accounts",
        rows,
        column_names=[
            "id",
            "customer_id",
            "account_type",
            "balance",
            "currency",
            "created_at",
        ],
    )


def load_transactions(client, df):
    import pandas as pd

    df["amount"] = pd.to_numeric(df["amount"])

    df["created_at"] = pd.to_datetime(
        df["created_at"],
        utc=True,
    )

    rows = []

    for row in df.itertuples(index=False):

        related_account_id = (
            None
            if pd.isna(row.related_account_id)
            else int(row.related_account_id)
        )

        rows.append(
            (
                int(row.id),
                int(row.account_id),
                str(row.txn_type),
                float(row.amount),
                related_account_id,
                str(row.status),
                row.created_at.to_pydatetime(),
            )
        )

    client.insert(
        "transactions",
        rows,
        column_names=[
            "id",
            "account_id",
            "txn_type",
            "amount",
            "related_account_id",
            "status",
            "created_at",
        ],
    )


def process_parquet_files():
    import pandas as pd

    s3 = get_minio_client()
    ch = get_clickhouse_client()

    ensure_control_table(ch)

    loaded_objects = get_loaded_objects(ch)

    paginator = s3.get_paginator("list_objects_v2")

    processed = 0
    skipped = 0

    for page in paginator.paginate(Bucket=MINIO_BUCKET):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            if key in loaded_objects:
                skipped += 1
                continue

            print(f"Loading: {key}")

            response = s3.get_object(
                Bucket=MINIO_BUCKET,
                Key=key,
            )

            data = response["Body"].read()

            df = pd.read_parquet(
                io.BytesIO(data)
            )

            table_name = key.split("/", 1)[0]

            if table_name == "customers":
                load_customers(ch, df)

            elif table_name == "accounts":
                load_accounts(ch, df)

            elif table_name == "transactions":
                load_transactions(ch, df)

            else:
                print(f"Skipping unknown object: {key}")
                continue

            ch.insert(
                "loaded_objects",
                [
                    (
                        key,
                        datetime.now(timezone.utc),
                    )
                ],
                column_names=[
                    "object_key",
                    "loaded_at",
                ],
            )

            processed += 1

    print(f"Processed: {processed}")
    print(f"Skipped already loaded: {skipped}")


with DAG(
    dag_id="minio_to_clickhouse",
    start_date=datetime(2026, 9, 13),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["banking", "minio", "clickhouse"],
) as dag:

    load_data = PythonOperator(
        task_id="load_minio_parquet",
        python_callable=process_parquet_files,
    )