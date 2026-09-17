import boto3
from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata
import json
import base64
from decimal import Decimal
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv(override=True)

def decode_decimal_fields(event):
    """
    Decode Kafka Connect Decimal values that were serialized
    as base64-encoded bytes.

    Current Debezium records using decimal.handling.mode=double
    are left unchanged.
    """
    schema = event.get("schema", {})
    payload = event.get("payload", {})

    schema_fields = schema.get("fields", [])

    # Find the 'after' schema
    after_schema = next(
        (
            field
            for field in schema_fields
            if field.get("field") == "after"
        ),
        None
    )

    if not after_schema:
        return payload.get("after")

    after_fields = after_schema.get("fields", [])
    record = payload.get("after")

    if not record:
        return record

    for field in after_fields:
        field_name = field.get("field")
        field_type = field.get("type")
        logical_type = field.get("name")
        parameters = field.get("parameters", {})

        if (
            field_name in record
            and field_type == "bytes"
            and logical_type == "org.apache.kafka.connect.data.Decimal"
        ):
            scale = int(parameters.get("scale", 0))

            raw_bytes = base64.b64decode(record[field_name])

            unscaled_value = int.from_bytes(
                raw_bytes,
                byteorder="big",
                signed=True
            )

            record[field_name] = float(
                Decimal(unscaled_value) / (Decimal(10) ** scale)
            )

    return record

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return

    df = pd.DataFrame([record["value"] for record in records])

    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'

    df.to_parquet(
        file_path,
        engine='fastparquet',
        index=False
    )

    s3_key = (
        f'{table_name}/date={date_str}/'
        f'{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    )

    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)

    print(
        f'Uploaded {len(records)} records '
        f'to s3://{bucket}/{s3_key}'
    )

    offsets = {}

    for record in records:
        tp = TopicPartition(
            record["topic"],
            record["partition"]
        )

        next_offset = record["offset"] + 1

        if tp not in offsets or next_offset > offsets[tp].offset:
            offsets[tp] = OffsetAndMetadata(
                next_offset,
                None
            )

    return offsets

# Batch consume
batch_size =20
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = decode_decimal_fields(event)  # Only take the actual row

    if record:
        buffer[topic].append(
            {
                "value": record,
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
            }
        )

    if len(buffer[topic]) >= batch_size:
        offsets = write_to_minio(topic.split('.')[-1], buffer[topic])
        consumer.commit(offsets=offsets)
        buffer[topic] = []