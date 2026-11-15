# Banking Data Stack

**An End-to-End Banking Data Engineering Platform | PostgreSQL • Kafka • Debezium • MinIO • Airflow • ClickHouse • dbt**

## Overview

The Banking Data Stack is an end-to-end data engineering project that demonstrates how to build a modern banking data platform using open-source technologies.

The platform generates synthetic banking data, captures database changes in near real time, streams events through Apache Kafka, stores data in an S3-compatible object storage system, and orchestrates ingestion into ClickHouse for analytical processing. Finally, dbt transforms the raw data into structured analytical models.

The project demonstrates key data engineering concepts, including Change Data Capture (CDC), event streaming, object storage, workflow orchestration, analytical data warehousing, and SQL-based data transformation.

## Architecture

The platform follows an event-driven data pipeline:

```text
                 PostgreSQL
              (Banking Database)
                      |
                      v
                  Debezium
               (Change Data Capture)
                      |
                      v
                   Kafka
                (Event Streaming)
                      |
                      v
                Kafka Consumer
                      |
                      v
                   MinIO
              (Parquet Storage)
                      |
                      v
                Apache Airflow
             (Pipeline Orchestration)
                      |
                      v
                 ClickHouse
              (Analytical Storage)
                      |
                      v
                    dbt
             (Data Transformation)
                      |
                      v
             Analytical Data Models
```

### Data Flow

1. **Data Generation:** Synthetic banking records are generated and inserted into PostgreSQL.
2. **Change Data Capture:** Debezium captures inserts and updates from PostgreSQL.
3. **Event Streaming:** Captured changes are published to Kafka topics.
4. **Object Storage:** A Kafka consumer reads events and writes them to MinIO in Parquet format.
5. **Data Ingestion:** Apache Airflow discovers and processes Parquet objects from MinIO.
6. **Analytical Storage:** The extracted data is loaded into ClickHouse.
7. **Data Transformation:** dbt transforms raw banking data into analytical models for reporting and analysis.

## Technology Stack

| Technology | Role |
|---|---|
| PostgreSQL | Source relational database |
| Python | Data generation, ingestion, and processing |
| Apache Kafka | Distributed event streaming |
| Debezium | PostgreSQL Change Data Capture |
| MinIO | S3-compatible object storage |
| Apache Airflow | Workflow orchestration and scheduling |
| Parquet | Columnar data storage format |
| ClickHouse | Analytical database |
| dbt | SQL-based data transformation |
| Docker | Containerization |
| Docker Compose | Infrastructure orchestration |
| Git & GitHub | Version control |

## Project Structure

```text
banking_datastack/
│
├── data_source/
│   └── postgres/
│       └── schema.sql
│
├── pipeline/
│   ├── data_generator/
│   │   └── data_generator.py
│   │
│   ├── kafka_debezium/
│   │   └── kafka_debezium_pipeline.py
│   │
│   └── consumer/
│       └── kafka_to_minio_pipeline.py
│
├── docker/
│   ├── dags/
│   │   └── minio_to_clickhouse.py
│   │
│   └── clickhouse/
│       ├── create_database.sql
│       └── init_clickhouse.py
│
├── transformations/
│   └── banking_datastack/
│       ├── models/
│       ├── dbt_project.yml
│       └── ...
│
├── repair_transactions.py
├── docker-compose.yml
├── docker-airflow.dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

*The structure above highlights the main components; additional configuration and supporting files may exist in the repository.*

## Prerequisites

Before setting up the project, ensure you have:

- Docker Desktop or Docker Engine
- Docker Compose V2
- Python 3.10 or later
- pip or uv
- Git

Recommended tools:

- Visual Studio Code
- PostgreSQL client (`psql`)
- dbt CLI
- MinIO Console or AWS CLI

## Getting Started

### 1. Clone the Repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd banking_datastack
```

### 2. Configure Environment Variables

Create a `.env` file in the project root. A sample configuration is provided in `.env.example`, where available.

```bash
cp .env.example .env
```

For Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Example configuration:

```env
# PostgreSQL Source Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=banking_db
POSTGRES_USER=banking_user
POSTGRES_PASSWORD=<your_password>

# Airflow Metadata Database
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=<your_password>
AIRFLOW_DB_NAME=airflow

# MinIO
MINIO_ROOT_USER=<your_minio_username>
MINIO_ROOT_PASSWORD=<your_minio_password>
MINIO_BUCKET=banking-data

# ClickHouse
CLICKHOUSE_DB=banking_db
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<your_password>

# Kafka
KAFKA_BOOTSTRAP=localhost:29092
KAFKA_GROUP=banking-group
```

Ensure that the variable names and values match the configuration expected by your Docker Compose file and Python scripts.

**Security:** Never commit `.env` or files containing credentials to GitHub. Use strong passwords and keep `.env.example` free of real secrets.

### 3. Install Python Dependencies

Create a virtual environment from the project root.

**Windows PowerShell**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Linux/macOS**

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The dbt ClickHouse adapter should also be available in the local Python environment.

Airflow dependencies must be installed in the Airflow Docker image when they are required by DAGs.

### 4. Build and Start the Infrastructure

The project uses Docker Compose to manage the infrastructure services, including:

- PostgreSQL
- Kafka and Zookeeper
- Debezium Kafka Connect
- MinIO
- ClickHouse
- Airflow metadata PostgreSQL
- Airflow Webserver
- Airflow Scheduler

Build the images:

```bash
docker compose build
```

Start the services:

```bash
docker compose up -d
```

Check their status:

```bash
docker compose ps
```

View service logs:

```bash
docker compose logs -f
```

### Service Endpoints

| Service | Local Address |
|---|---|
| PostgreSQL | `localhost:5432` |
| Kafka Broker | `localhost:29092` |
| Kafka Connect | `http://localhost:8083` |
| MinIO API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |
| ClickHouse HTTP | `http://localhost:8123` |
| ClickHouse Native | `localhost:9002` |
| Airflow Webserver | `http://localhost:8080` |

These endpoints assume the ports are published as configured in `docker-compose.yml`.

### 5. Initialize PostgreSQL

The banking database schema is defined in:

```text
data_source/postgres/schema.sql
```

Apply the schema using the PostgreSQL container:

```bash
docker compose exec postgres psql -U banking_user -d banking_db -f /docker-entrypoint-initdb.d/schema.sql
```

Alternatively, if PostgreSQL is accessible from your host:

```bash
psql postgresql://banking_user:<your_password>@localhost:5432/banking_db -f data_source/postgres/schema.sql
```

The schema defines three primary banking entities:

- `customers`
- `accounts`
- `transactions`

The relationships between these tables represent customers, their accounts, and associated financial transactions.

### 6. Register the Debezium Connector

Debezium captures database changes from PostgreSQL and publishes them to Kafka.

Run the connector configuration script:

```bash
python pipeline/kafka_debezium/kafka_debezium_pipeline.py
```

The connector is configured to capture changes from:

```text
public.customers
public.accounts
public.transactions
```

Check the registered connectors:

```bash
curl http://localhost:8083/connectors
```

Check the connector status:

```bash
curl http://localhost:8083/connectors/<connector-name>/status
```

Ensure the connector is running before generating data that you want captured.

### 7. Generate Synthetic Banking Data

The data generator creates synthetic customer, account, and transaction records in PostgreSQL.

Run a single generation cycle:

```bash
python pipeline/data_generator/data_generator.py --once
```

Run continuously:

```bash
python pipeline/data_generator/data_generator.py
```

The generated data provides a continuous stream of database changes for Debezium to capture.

### 8. Stream Kafka Events into MinIO

The Kafka consumer reads events from the banking Kafka topics and stores them as Parquet files in MinIO.

Start the consumer:

```bash
python pipeline/consumer/kafka_to_minio_pipeline.py
```

The script creates the `banking-data` bucket if it does not already exist and writes objects using an entity-based partition structure.

Example:

```text
banking-data/
│
├── customers/
│   └── date=YYYY-MM-DD/
│       └── customers_<timestamp>.parquet
│
├── accounts/
│   └── date=YYYY-MM-DD/
│       └── accounts_<timestamp>.parquet
│
└── transactions/
    └── date=YYYY-MM-DD/
        └── transactions_<timestamp>.parquet
```

Inspect the stored files through the MinIO Console:

```text
http://localhost:9001
```

Use the credentials configured in your `.env` file.

### 9. Load Data into ClickHouse with Airflow

Apache Airflow orchestrates the ingestion of Parquet files from MinIO into ClickHouse.

The DAG is located at:

```text
docker/dags/minio_to_clickhouse.py
```

Access the Airflow interface:

```text
http://localhost:8080
```

If the Airflow metadata database requires migration, run:

```bash
docker compose exec airflow-webserver airflow db migrate
```

Locate the DAG:

```text
minio_to_clickhouse
```

Enable the DAG or trigger a manual run.

The DAG performs the following operations:

1. Connects to MinIO.
2. Discovers available Parquet objects.
3. Downloads and reads the files.
4. Processes the records and handles relevant data types.
5. Inserts the records into ClickHouse.
6. Tracks processed objects using the `loaded_objects` table.

The target ClickHouse database is:

```text
banking_db
```

The primary raw tables are:

```text
customers
accounts
transactions
```

### 10. Transform Data with dbt

The dbt project is located at:

```text
transformations/banking_datastack/
```

Navigate to the project directory:

```bash
cd transformations/banking_datastack
```

Install dbt dependencies:

```bash
dbt deps
```

Validate the project:

```bash
dbt debug
```

Run the transformation models:

```bash
dbt run
```

Execute data quality tests:

```bash
dbt test
```

The dbt profile should be named:

```text
banking_datastack
```

Ensure `profiles.yml` is configured with the ClickHouse connection details and points to the correct database.

The resulting dbt models provide a structured analytical layer on top of the raw banking data.

## Data Validation

After running the pipeline, validate that records are moving through each component.

### PostgreSQL

Check the source transaction count:

```sql
SELECT COUNT(*) FROM transactions;
```

Or run:

```bash
psql postgresql://banking_user:<your_password>@localhost:5432/banking_db -c "SELECT COUNT(*) FROM transactions;"
```

### Kafka

List Kafka topics from the broker container:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Check that the expected banking topics are receiving messages using the Kafka console consumer or your preferred Kafka monitoring tool.

### MinIO

List stored Parquet objects:

```bash
aws --endpoint-url http://localhost:9000 s3 ls s3://banking-data --recursive
```

### ClickHouse

Connect to ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client
```

Inspect the database:

```sql
SHOW TABLES FROM banking_db;
```

Check the loaded records:

```sql
SELECT COUNT(*) FROM banking_db.customers;

SELECT COUNT(*) FROM banking_db.accounts;

SELECT COUNT(*) FROM banking_db.transactions;
```

### dbt

Run:

```bash
dbt test
```

Review the generated model tables and test results to confirm that the analytical layer is populated correctly.

## Common Troubleshooting

### Docker Services Fail to Start

Check Docker Desktop and inspect the container status:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

### PostgreSQL Connection Errors

- Verify the PostgreSQL container is running.
- Confirm database credentials.
- Check that the database and schema have been initialized.
- Use `localhost` for host-side connections and the PostgreSQL service name for container-side connections.

### Kafka or Debezium Not Capturing Changes

- Confirm Kafka and Kafka Connect are running.
- Check the Debezium connector status.
- Verify PostgreSQL logical replication and replication permissions.
- Inspect Kafka Connect logs:

```bash
docker compose logs -f connect
```

### MinIO Objects Not Appearing

- Ensure the Kafka consumer is running.
- Confirm the correct Kafka bootstrap address.
- Verify MinIO credentials and bucket configuration.
- Inspect the MinIO Console and consumer logs.

### Airflow DAG Not Appearing

Check scheduler logs:

```bash
docker compose logs -f airflow-scheduler
```

Check DAG import errors:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Confirm that the DAG file is mounted in the expected Airflow DAG directory.

### ClickHouse Loading Errors

Check ClickHouse logs:

```bash
docker compose logs -f clickhouse
```

Verify that the ClickHouse service is reachable and that the target database and tables exist.

### dbt Connection Errors

Run:

```bash
dbt debug
```

Verify the ClickHouse host, port, credentials, database, and profile configuration.

## Useful Docker Commands

Start the infrastructure:

```bash
docker compose up -d
```

Rebuild and start:

```bash
docker compose up -d --build
```

View running services:

```bash
docker compose ps
```

Follow logs:

```bash
docker compose logs -f
```

Restart a service:

```bash
docker compose restart <service-name>
```

Stop containers without deleting persistent volumes:

```bash
docker compose down
```

**Note:** Avoid `docker compose down -v` unless you intentionally want to delete persistent data stored in Docker volumes.

## Future Enhancements

Potential improvements to the platform include:

- Implementing automated data quality checks.
- Building additional dbt models for banking analytics.
- Improving incremental loading and duplicate prevention.
- Adding pipeline monitoring, alerting, and observability.
- Implementing schema evolution and data validation.
- Introducing CI/CD for automated testing and deployment.
- Applying production-grade secrets management and access controls.
- Connecting BI tools for dashboards and business intelligence.

## Key Learning Outcomes

This project demonstrates practical experience in:

- Designing end-to-end data engineering architectures.
- Implementing Change Data Capture with Debezium.
- Building event-driven pipelines using Apache Kafka.
- Processing and storing data in Parquet format.
- Implementing S3-compatible object storage with MinIO.
- Orchestrating data workflows using Apache Airflow.
- Loading analytical data into ClickHouse.
- Developing SQL transformations and tests with dbt.
- Containerizing distributed services with Docker Compose.
- Managing collaborative development with Git and GitHub.

## Author

**Oluwatosin Akinbinu**

Data Engineering | Banking Data Stack

GitHub: [Your GitHub Profile](<https://github.com/Seniormanx>)

---

*This project is intended for educational and demonstration purposes. It uses synthetic banking data and should not be used to process real customer financial information without appropriate security, privacy, and regulatory controls.*