import boto3
import os
import io
import base64
import pandas as pd
from decimal import Decimal

s3 = boto3.client(
    's3',
    endpoint_url=os.environ['MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['MINIO_ROOT_USER'],
    aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD']
)

bucket = os.environ['MINIO_BUCKET']

objects = s3.list_objects_v2(
    Bucket=bucket,
    Prefix='transactions/date=2026-09-13/'
).get('Contents', [])

repaired = 0

for obj in objects:
    key = obj['Key']

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    df = pd.read_parquet(
        io.BytesIO(response['Body'].read()),
        engine='pyarrow'
    )

    print(f'Processing: {key}')

    df['amount'] = [
        float(
            Decimal(
                int.from_bytes(
                    base64.b64decode(value),
                    byteorder='big',
                    signed=True
                )
            ) / Decimal(100)
        )
        for value in df['amount']
    ]

    output = io.BytesIO()

    df.to_parquet(
        output,
        engine='pyarrow',
        index=False
    )

    output.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=output.getvalue()
    )

    repaired += 1

print(f'Repaired {repaired} files')
