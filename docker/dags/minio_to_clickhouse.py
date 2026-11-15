
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
import logging
import os

import boto3
import clickhouse_connect
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]

CLICKHOUSE_HOST = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]

CLICKHOUSE_DB = "banking_db"

TABLES = {"customers", "accounts", "transactions"}

logger = logging.getLogger(__name__)


# ============================================================
# CONNECTIONS
# ============================================================

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="default",
        connect_timeout=10,
        send_receive_timeout=300,
    )


# ============================================================
# CLICKHOUSE DATABASE AND TABLES
# ============================================================

def ensure_clickhouse_tables(client):

    client.command(
        f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}"
    )

    client.command("""
        CREATE TABLE IF NOT EXISTS banking_db.customers
        (
            id UInt64,
            first_name String,
            last_name String,
            email String,
            created_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree()
        ORDER BY id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS banking_db.accounts
        (
            id UInt64,
            customer_id UInt64,
            account_type LowCardinality(String),
            balance Decimal(18, 2),
            currency FixedString(3),
            created_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree()
        ORDER BY (customer_id, id)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS banking_db.transactions
        (
            id UInt64,
            account_id UInt64,
            txn_type LowCardinality(String),
            amount Decimal(18, 2),
            related_account_id Nullable(UInt64),
            status LowCardinality(String),
            created_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (account_id, created_at, id)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS banking_db.loaded_objects
        (
            object_key String,
            etag String,
            loaded_at DateTime64(6, 'UTC')
        )
        ENGINE = MergeTree()
        ORDER BY (object_key, etag)
    """)

    logger.info("ClickHouse database and tables are ready.")


# ============================================================
# FILE TRACKING
# ============================================================

def get_loaded_objects(client):

    rows = client.query("""
        SELECT object_key, etag
        FROM banking_db.loaded_objects
    """).result_rows

    return set(rows)


def mark_object_loaded(client, key, etag):

    client.insert(
        "banking_db.loaded_objects",
        [(key, etag, datetime.now(timezone.utc))],
        column_names=[
            "object_key",
            "etag",
            "loaded_at",
        ],
    )


# ============================================================
# DATA VALIDATION AND CONVERSION
# ============================================================

def validate_columns(df, required_columns, key):

    missing = set(required_columns) - set(df.columns)

    if missing:
        raise ValueError(
            f"{key}: Missing required columns: {sorted(missing)}"
        )

    if df.empty:
        raise ValueError(f"{key}: Parquet file contains no rows.")


def convert_id(value, column_name):

    if pd.isna(value):
        raise ValueError(f"{column_name} cannot be NULL")

    result = int(value)

    if result < 0:
        raise ValueError(f"{column_name} cannot be negative")

    return result


def convert_decimal(value, column_name):

    if pd.isna(value):
        raise ValueError(f"{column_name} cannot be NULL")

    try:
        result = Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        raise ValueError(
            f"Invalid decimal value in {column_name}: {value}"
        )

    if not result.is_finite():
        raise ValueError(f"{column_name} must be finite")

    if abs(result) >= Decimal("10000000000000000"):
        raise ValueError(
            f"{column_name} exceeds Decimal(18,2) capacity"
        )

    return result


def convert_datetime(value, column_name):

    if pd.isna(value):
        raise ValueError(f"{column_name} cannot be NULL")

    result = pd.to_datetime(value, utc=True, errors="raise")

    return result.to_pydatetime()


def convert_string(value, column_name):

    if pd.isna(value):
        raise ValueError(f"{column_name} cannot be NULL")

    return str(value)


# ============================================================
# LOAD CUSTOMERS
# ============================================================

def load_customers(client, df, key):

    columns = [
        "id",
        "first_name",
        "last_name",
        "email",
        "created_at",
    ]

    validate_columns(df, columns, key)

    rows = []

    for row in df.itertuples(index=False):

        rows.append((
            convert_id(row.id, "id"),
            convert_string(row.first_name, "first_name"),
            convert_string(row.last_name, "last_name"),
            convert_string(row.email, "email"),
            convert_datetime(row.created_at, "created_at"),
        ))

    client.insert(
        "banking_db.customers",
        rows,
        column_names=columns,
    )

    logger.info("Loaded %s customer records.", len(rows))


# ============================================================
# LOAD ACCOUNTS
# ============================================================

def load_accounts(client, df, key):

    columns = [
        "id",
        "customer_id",
        "account_type",
        "balance",
        "currency",
        "created_at",
    ]

    validate_columns(df, columns, key)

    rows = []

    for row in df.itertuples(index=False):

        currency = convert_string(row.currency, "currency")

        if len(currency) != 3:
            raise ValueError(
                f"Invalid currency code: {currency}"
            )

        rows.append((
            convert_id(row.id, "id"),
            convert_id(row.customer_id, "customer_id"),
            convert_string(row.account_type, "account_type"),
            convert_decimal(row.balance, "balance"),
            currency,
            convert_datetime(row.created_at, "created_at"),
        ))

    client.insert(
        "banking_db.accounts",
        rows,
        column_names=columns,
    )

    logger.info("Loaded %s account records.", len(rows))


# ============================================================
# LOAD TRANSACTIONS
# ============================================================

def load_transactions(client, df, key):

    columns = [
        "id",
        "account_id",
        "txn_type",
        "amount",
        "related_account_id",
        "status",
        "created_at",
    ]

    validate_columns(df, columns, key)

    rows = []

    for row in df.itertuples(index=False):

        related_account_id = (
            None
            if pd.isna(row.related_account_id)
            else convert_id(
                row.related_account_id,
                "related_account_id",
            )
        )

        amount = convert_decimal(row.amount, "amount")

        if amount <= 0:
            raise ValueError("Transaction amount must be positive")

        rows.append((
            convert_id(row.id, "id"),
            convert_id(row.account_id, "account_id"),
            convert_string(row.txn_type, "txn_type"),
            amount,
            related_account_id,
            convert_string(row.status, "status"),
            convert_datetime(row.created_at, "created_at"),
        ))

    client.insert(
        "banking_db.transactions",
        rows,
        column_names=columns,
    )

    logger.info("Loaded %s transaction records.", len(rows))


# ============================================================
# PROCESS MINIO PARQUET FILES
# ============================================================

def process_parquet_files():

    s3 = get_minio_client()
    ch = get_clickhouse_client()

    processed = 0
    skipped = 0

    try:

        ensure_clickhouse_tables(ch)

        loaded_objects = get_loaded_objects(ch)

        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=MINIO_BUCKET):

            for obj in page.get("Contents", []):

                key = obj["Key"]

                if not key.lower().endswith(".parquet"):
                    continue

                # Expected object keys:
                # customers/customers.parquet
                # accounts/accounts.parquet
                # transactions/transactions.parquet

                table_name = key.split("/", 1)[0].lower()

                if table_name not in TABLES:
                    logger.warning(
                        "Ignoring unknown Parquet object: %s",
                        key,
                    )
                    continue

                etag = obj.get("ETag", "").strip('"')

                if not etag:
                    raise ValueError(
                        f"MinIO did not return an ETag for {key}"
                    )

                if (key, etag) in loaded_objects:

                    logger.info(
                        "Skipping previously loaded file: %s",
                        key,
                    )

                    skipped += 1
                    continue

                logger.info("Downloading %s from MinIO.", key)

                response = s3.get_object(
                    Bucket=MINIO_BUCKET,
                    Key=key,
                )

                data = response["Body"].read()

                if not data:
                    raise ValueError(
                        f"Downloaded Parquet file is empty: {key}"
                    )

                df = pd.read_parquet(BytesIO(data))

                logger.info(
                    "Downloaded %s rows from %s.",
                    len(df),
                    key,
                )

                if table_name == "customers":
                    load_customers(ch, df, key)

                elif table_name == "accounts":
                    load_accounts(ch, df, key)

                elif table_name == "transactions":
                    load_transactions(ch, df, key)

                # Only mark a file after its data insert succeeds.
                mark_object_loaded(ch, key, etag)

                loaded_objects.add((key, etag))

                processed += 1

                logger.info("Successfully processed %s.", key)

        logger.info(
            "Pipeline completed. Processed=%s, Skipped=%s",
            processed,
            skipped,
        )

    finally:
        ch.close()


# ============================================================
# AIRFLOW DAG
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="minio_to_clickhouse",
    default_args=default_args,
    description="Load banking Parquet files from MinIO into ClickHouse",
    start_date=datetime(2026, 9, 13, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=40),
    tags=["banking", "minio", "clickhouse", "etl"],
) as dag:

    load_minio_parquet = PythonOperator(
        task_id="load_minio_parquet",
        python_callable=process_parquet_files,
        execution_timeout=timedelta(minutes=30),
    )
    