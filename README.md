Banking Data Stack

This project builds a small end-to-end banking data platform using PostgreSQL, Kafka, Debezium, MinIO, Airflow, ClickHouse, and dbt. It generates fake banking data, streams it through Kafka, lands it in MinIO as Parquet files, loads it into ClickHouse, and transforms it with dbt for analytics.

The full data flow looks like this:

PostgreSQL -> Debezium -> Kafka -> Kafka consumer -> MinIO -> Airflow DAG -> ClickHouse -> dbt models

## Project structure

- `data_source/postgres/schema.sql` — database schema for customers, accounts, and transactions
- `pipeline/data_generator/data_generator.py` — generates synthetic banking data into PostgreSQL
- `pipeline/kafka_debezium/kafka_debezium_pipeline.py` — creates the Debezium PostgreSQL connector
- `pipeline/consumer/kafka_to_minio_pipeline.py` — reads Kafka events and stores them as Parquet files in MinIO
- `docker/dags/minio_to_clickhouse.py` — Airflow DAG that loads Parquet files from MinIO into ClickHouse
- `transformations/banking_datastack/` — dbt project used to transform the loaded data
- `repair_transactions.py` — repairs parquet transaction values when needed
- `docker-compose.yml` — all required services for the stack

## Prerequisites

Before starting, make sure you have:

- Docker Desktop or Docker Engine installed and running
- Docker Compose v2
- Python 3.10+
- `pip` or `uv`
- Git

Optional but helpful:

- PostgreSQL client tools (`psql`)
- dbt CLI
- MinIO client or browser access

## 1. Clone the repository

```bash
git clone <your-repo-url>
cd banking_datastack
```

## 2. Create environment variables

Create a `.env` file in the project root. This file is required by Docker Compose and the Python scripts.

Example:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=banking_db
POSTGRES_USER=banking_user
POSTGRES_PASSWORD=banking_pass

AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=banking-data

CLICKHOUSE_DB=banking
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

KAFKA_BOOTSTRAP=localhost:29092
KAFKA_GROUP=banking-groupn  
```

A sample file is provided in `.env.example` if you want to copy from it.

## 3. Install Python dependencies

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# or .venv\Scripts\activate  # Windows PowerShell

pip install -r requirements.txt
```

If you use `uv`, you can also do:

```bash
uv sync
```

## 4. Start the infrastructure services

This project uses Docker Compose to run:

- PostgreSQL
- Kafka + Zookeeper
- Debezium Connect
- MinIO
- ClickHouse
- Airflow Postgres
- Airflow Webserver
- Airflow Scheduler

Start everything with:

```bash
docker compose up -d
```

Check that the services are running:

```bash
docker compose ps
```

Useful ports:

- PostgreSQL: `5432`
- Kafka broker: `29092`
- Kafka Connect: `8083`
- MinIO API: `9000`
- MinIO Console: `9001`
- ClickHouse HTTP: `8123`
- ClickHouse native: `9002`
- Airflow UI: `8080`

## 5. Initialize the PostgreSQL database

The schema is stored in `data_source/postgres/schema.sql`.

Apply it to the PostgreSQL container:

```bash
docker compose exec postgres psql -U banking_user -d banking_db -f /docker-entrypoint-initdb.d/schema.sql
```

If the script is not automatically mounted, run the following manually from the repo:

```bash
psql postgresql://banking_user:banking_pass@localhost:5432/banking_db -f data_source/postgres/schema.sql
```

The schema creates:

- `customers`
- `accounts`
- `transactions`

## 6. Generate fake banking data

The data generator inserts records into PostgreSQL and keeps looping unless you pass `--once`.

Run it once:

```bash
python pipeline/data_generator/data_generator.py --once
```

Run it continuously:

```bash
python pipeline/data_generator/data_generator.py
```

This produces transactions, customer records, and account balances in the database.

## 7. Register the Debezium connector

The Debezium connector listens for changes in the PostgreSQL tables and pushes them into Kafka topics.

Run:

```bash
python pipeline/kafka_debezium/kafka_debezium_pipeline.py
```

This creates the connector for:

- `public.customers`
- `public.accounts`
- `public.transactions`

You can confirm the connector is active via:

```bash
curl http://localhost:8083/connectors
```

## 8. Consume Kafka messages into MinIO

The consumer script reads messages from Kafka and writes them as Parquet files into MinIO.

Run:

```bash
python pipeline/consumer/kafka_to_minio_pipeline.py
```

This script creates a bucket named `banking-data` if it does not already exist and stores objects under a path like:

```text
customers/date=YYYY-MM-DD/customers_123456.parquet
accounts/date=YYYY-MM-DD/accounts_123456.parquet
transactions/date=YYYY-MM-DD/transactions_123456.parquet
```

You can inspect the bucket in the MinIO console at:

- `http://localhost:9001`

Default login:

- Username: `minioadmin`
- Password: `minioadmin`

## 9. Fix transaction parquet values (if needed)

The `repair_transactions.py` script decodes and repairs transaction `amount` values stored in Parquet files before loading them into ClickHouse.

Run:

```bash
python repair_transactions.py
```

This is useful when transaction amounts were encoded as base64/decimal values and need to be converted back to normal numeric values.

## 10. Start Airflow and load data into ClickHouse

The Airflow DAG at `docker/dags/minio_to_clickhouse.py` reads the parquet files from MinIO and inserts them into the ClickHouse database.

Access the Airflow UI:

```text
http://localhost:8080
```

Default Airflow credentials are usually:

- Username: `airflow`
- Password: `airflow`

If the database is not initialized yet, run:

```bash
docker compose exec airflow-webserver airflow db init
```

Then start or trigger the DAG named:

```text
minio_to_clickhouse
```

This DAG loads the transformed data into ClickHouse tables named:

- `customers`
- `accounts`
- `transactions`

## 11. Run dbt transformations

The dbt project lives in `transformations/banking_datastack`.

Activate your environment and go to the project directory:

```bash
cd transformations/banking_datastack
```

Initialize dependencies and build models:

```bash
dbt deps
dbt run
dbt test
```

If you are using a profile, make sure `profiles.yml` is configured for ClickHouse and points at your database. The project currently expects a profile named `banking_datastack`.

## 12. Verify the end-to-end flow

Once everything is running, verify each layer:

- PostgreSQL contains generated data
- Kafka topics are receiving Debezium changes
- MinIO contains Parquet files
- Airflow DAG loads files into ClickHouse
- dbt models create the final analytical layer

Example checks:

```bash
psql postgresql://banking_user:banking_pass@localhost:5432/banking_db -c "SELECT COUNT(*) FROM transactions;"
```

Check MinIO contents in the UI or with AWS CLI:

```bash
aws --endpoint-url http://localhost:9000 s3 ls s3://banking-data --recursive
```

Check ClickHouse:

```bash
curl "http://localhost:8123/?user=default&password=&database=banking" -d "SHOW TABLES"
```

## Common troubleshooting

### Docker services fail to start

- Make sure Docker Desktop is running
- Check logs:

```bash
docker compose logs -f
```

### PostgreSQL connection errors

- Confirm the `.env` variables match the values used in Docker Compose
- Verify the database was created and the schema was applied

### Kafka/Debezium not receiving changes

- Confirm the Debezium connector is active
- Check Kafka Connect logs:

```bash
docker compose logs connect
```

### MinIO files not appearing

- Ensure the Kafka consumer is still running
- Check that the bucket exists and permissions are correct

### Airflow DAG not loading

- Confirm the webserver and scheduler containers started successfully
- Run:

```bash
docker compose logs airflow-scheduler
```

### dbt connection issues

- Ensure ClickHouse is reachable on `localhost:8123`
- Check `profiles.yml` and the `dbt_project.yml` settings

## Recommended run order

To bring the project up in a working order, use this sequence:

1. `docker compose up -d`
2. Apply the PostgreSQL schema
3. Start the data generator
4. Register the Debezium connector
5. Start the Kafka consumer to MinIO
6. Trigger the Airflow DAG to load into ClickHouse
7. Run dbt models

## Notes

This project is a good example of a lightweight streaming analytics stack for banking data. It is intentionally simple and easy to extend. You can add:

- monitoring and alerts
- more complex dbt transformations
- data quality checks
- orchestration and retries
- S3/MinIO lifecycle policies
- real-world production-grade security and secrets management

## Useful commands summary

```bash
docker compose up -d
docker compose ps
docker compose logs -f

python pipeline/data_generator/data_generator.py --once
python pipeline/kafka_debezium/kafka_debezium_pipeline.py
python pipeline/consumer/kafka_to_minio_pipeline.py
python repair_transactions.py

cd transformations/banking_datastack
dbt deps
dbt run
dbt test
```
