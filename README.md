# Banking Data Stack

An end-to-end banking data engineering and analytics platform built with PostgreSQL, Debezium, Kafka, MinIO, Apache Airflow, ClickHouse, dbt, and Metabase.

This project demonstrates how synthetic banking data can be generated, captured through Change Data Capture (CDC), streamed through Kafka, stored as Parquet files in MinIO, loaded into ClickHouse, transformed with dbt, and visualized through interactive Metabase dashboards.

## Project Architecture

The platform follows this data flow:

**PostgreSQL → Debezium → Kafka → Kafka Consumer → MinIO → Airflow → ClickHouse → dbt → Metabase**

![End-to-End Banking Data Stack Architecture](https://github.com/user-attachments/assets/bd968a43-4eed-4617-9023-480f47f2f6ee)

### Architecture Overview

1. **PostgreSQL:** Stores the source banking data, including customers, accounts, and transactions.
2. **Debezium:** Captures database changes using Change Data Capture (CDC).
3. **Kafka:** Receives and distributes the change events.
4. **Kafka Consumer:** Reads Kafka messages and writes the data into Parquet files.
5. **MinIO:** Provides S3-compatible object storage for the Parquet files.
6. **Apache Airflow:** Orchestrates the ingestion of Parquet files from MinIO into ClickHouse.
7. **ClickHouse:** Stores the ingested banking data and analytical tables.
8. **dbt:** Performs data cleaning, transformation, and analytical modelling.
9. **Metabase:** Connects to ClickHouse and provides interactive dashboards for banking analytics.

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
│   ├── clickhouse/
│   ├── minio/
│   ├── postgres/
│   └── ...
│
├── transformations/
│   └── banking_datastack/
│       └── dbt models and configuration
│
├── screenshots/
│   └── banking_datastack_metabase_dashboard_page_1.png
│
├── repair_transactions.py
├── docker-compose.yml
├── docker-airflow.dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

### Main Components

| Component | Purpose |
|---|---|
| PostgreSQL | Source banking database |
| Debezium | Change Data Capture |
| Kafka | Event streaming |
| MinIO | Parquet object storage |
| Airflow | Data pipeline orchestration |
| ClickHouse | Analytical database |
| dbt | Data transformation |
| Metabase | Business intelligence and dashboards |

## Prerequisites

Before setting up the project, ensure you have:

- Docker Desktop or Docker Engine installed and running
- Docker Compose v2
- Python 3.10+
- pip or uv
- Git

Recommended tools:

- Visual Studio Code
- PostgreSQL client (`psql`)
- dbt CLI
- MinIO Console or AWS CLI

## Getting Started

## 1. Clone the Repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd banking_datastack
```

## 2. Configure Environment Variables

Create a `.env` file in the project root.

You can use `.env.example` as a starting point.

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
POSTGRES_PASSWORD=your_password

# Airflow Metadata Database
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=your_airflow_password
AIRFLOW_DB_NAME=airflow

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_minio_password
MINIO_BUCKET=banking-data

CLICKHOUSE_DB=banking_db
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_clickhouse_password

# Kafka
KAFKA_BOOTSTRAP=localhost:29092
KAFKA_GROUP=banking-group

MB_SESSION_SECRET_KEY=your_random_session_secret
```

Use the same values consistently across your Docker Compose configuration and application scripts.

Do not commit your actual `.env` file or production credentials to GitHub.

## 3. Install Python Dependencies

From the project root, create and activate a virtual environment.

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Linux/macOS

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The `dbt-clickhouse` adapter is installed in this environment. Airflow runs in Docker Compose, so Airflow-specific dependencies must be installed in the Airflow image.

## 4. Start the Infrastructure Services

Docker Compose runs the following services:

- PostgreSQL
- Zookeeper
- Kafka
- Debezium Kafka Connect
- MinIO
- ClickHouse
- Airflow PostgreSQL
- Airflow Webserver
- Airflow Scheduler
- Metabase

Start the infrastructure:

```bash
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

### Service Ports

| Service | Port | Access |
|---|---:|---|
| PostgreSQL | 5432 | `localhost:5432` |
| Kafka | 29092 | `localhost:29092` |
| Kafka Connect | 8083 | `localhost:8083` |
| MinIO API | 9000 | `localhost:9000` |
| MinIO Console | 9001 | `http://localhost:9001` |
| ClickHouse HTTP | 8123 | `localhost:8123` |
| ClickHouse Native | 9002 | `localhost:9002` |
| Airflow | 8080 | `http://localhost:8080` |
| Metabase | 3000 | `http://localhost:3000` |

## 5. Initialize the PostgreSQL Database

The source database schema is located at:

```text
data_source/postgres/schema.sql
```

Apply the schema to PostgreSQL:

```bash
psql postgresql://banking_user:your_password@localhost:5432/banking_db -f data_source/postgres/schema.sql
```

The schema creates three main tables:

- `customers`
- `accounts`
- `transactions`

## 6. Generate Synthetic Banking Data

The data generator inserts synthetic banking records into PostgreSQL.

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

The generator produces customer records, account information, and banking transactions.

## 7. Register the Debezium Connector

Debezium captures changes from PostgreSQL and publishes them to Kafka topics.

Check the connector status:

```bash
curl http://localhost:8083/connectors/<connector-name>/status
```

The connector captures changes from:

### 7. Generate Synthetic Banking Data

Check the registered connectors:

```bash
python pipeline/data_generator/data_generator.py --once
```

## 8. Stream Kafka Events into MinIO

The Kafka consumer reads messages from Kafka and writes them as Parquet files into MinIO.

The generated data provides a continuous stream of database changes for Debezium to capture.

### 8. Stream Kafka Events into MinIO

The Kafka consumer reads events from the banking Kafka topics and stores them as Parquet files in MinIO.

Start the consumer:

```bash
python pipeline/consumer/kafka_to_minio_pipeline.py
```

The script creates the `banking-data` bucket if it does not already exist.

Objects are stored using date-partitioned paths:

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

Access the MinIO console:

```text
http://localhost:9001
```

Sign in using the MinIO credentials configured in your `.env` file.

## 9. Repair Transaction Parquet Values (If Required)

The `repair_transactions.py` script repairs transaction amount values in Parquet files when they have been encoded as base64/decimal values.

The DAG is located at:

```text
docker/dags/minio_to_clickhouse.py
```

This step is only necessary when the stored transaction values require repair before ingestion.

## 10. Load Data into ClickHouse with Airflow

The Airflow DAG is located at:

```text
docker/dags/minio_to_clickhouse.py
```

It reads Parquet files from MinIO and loads them into ClickHouse.

Access the Airflow interface:

```text
http://localhost:8080
```

The DAG is named:

```text
minio_to_clickhouse
```

Trigger the DAG from the Airflow interface to load the data.

The ingestion pipeline populates the following ClickHouse tables:

- `customers`
- `accounts`
- `transactions`

Airflow orchestrates this ingestion process and provides visibility into task execution and pipeline status.

## 11. Transform Data with dbt

The dbt project is located at:

```text
transformations/banking_datastack
```

Navigate to the project directory:

```bash
cd transformations/banking_datastack
```

Install dbt dependencies:

```bash
dbt deps
```

Run transformations:

```bash
dbt run
```

Run data quality tests:

```bash
dbt test
```

The dbt profile must be configured to connect to ClickHouse using the `dbt-clickhouse` adapter.

The project expects a profile named:

```text
banking_datastack
```

### Medallion Architecture

The transformation layer can be organized into three logical layers:

| Layer | Purpose |
|---|---|
| Bronze | Raw ingested banking data |
| Silver | Cleaned, validated, and standardized data |
| Gold | Business-ready analytical models |

This layered approach separates raw ingestion from data preparation and business reporting.

The Gold layer is the recommended source for Metabase dashboards because it provides data models designed for analytical queries.

## 12. Connect Metabase to ClickHouse

Metabase is used as the business intelligence and visualization layer of the platform.

It connects directly to ClickHouse to explore the transformed banking data and build interactive dashboards.

### Start Metabase

Metabase is included in the Docker Compose configuration.

Start it independently if the other services are already running:

```bash
docker compose up -d metabase
```

Check its status:

```bash
docker compose ps
```

Open the Metabase web interface:

```text
http://localhost:3000
```

Complete the initial setup and create your Metabase administrator account.

### Metabase Application Storage

Metabase uses an embedded H2 application database for this development setup.

The application database is persisted through a Docker named volume:

```yaml
volumes:
  - metabase_data:/metabase-data
```

This stores Metabase application information, including users, dashboards, and saved questions.

The volume is separate from the ClickHouse database, which stores the actual banking data.

### Add ClickHouse as a Database

Inside Metabase:

1. Open **Admin settings**.
2. Navigate to **Databases**.
3. Click **Add a database**.
4. Select **ClickHouse**.
5. Enter the connection details.

Use the following configuration:

| Setting | Value |
|---|---|
| Database type | ClickHouse |
| Display name | Banking ClickHouse |
| Host | `clickhouse` |
| Port | `8123` |
| Database name | Your configured `CLICKHOUSE_DB` |
| Username | Your ClickHouse username |
| Password | Your ClickHouse password |
| SSL | Disabled for this local setup |

**Important:** Use `clickhouse` as the hostname because Metabase and ClickHouse communicate over the Docker Compose network. Do not use `localhost` inside the Metabase container.

Use port `8123` for the ClickHouse HTTP interface, not the native port `9002`.

Click **Save** to establish the connection.

Once connected, Metabase can discover the available ClickHouse tables and views.

## 13. Create the Banking Analytics Dashboard

After connecting ClickHouse, use Metabase to create visualizations from your banking data.

Create a new dashboard:

1. Open the Metabase interface.
2. Select **+ New**.
3. Choose **Dashboard**.
4. Name it `banking_datastack`.
5. Add saved questions and visualizations from your ClickHouse database.
6. Arrange the visualizations into a dashboard layout.
7. Save the dashboard.

### Dashboard Visualizations

The banking dashboard includes the following analytical views:

#### 1. Transaction Overview

Provides an overview of banking transaction activity.

- Transaction counts by transaction type
- Completed transaction counts
- Failed transaction counts
- Completed transaction values
- Failed transaction values

#### 2. Customer Overview

Displays the total number of customers in the banking database.

#### 3. Account Overview

Displays the total number of banking accounts.

#### 4. Total Account Balance

A horizontal bar chart comparing total account balances by account type.

#### 5. Largest Withdrawals

Highlights accounts associated with the largest withdrawal transactions.

#### 6. Largest Transfers

Highlights accounts associated with the largest transfer transactions.

#### 7. Largest Deposits

Highlights accounts associated with the largest deposit transactions.

#### 8. Customer Summary

A detailed table containing customer and account information, including:

- Customer ID
- Customer name
- Email
- Account type
- Account balance
- Number of accounts
- Total withdrawals
- Total transactions

The dashboard also includes interactive filters, such as account type and customer name, to support data exploration.

### Final Metabase Dashboard

The following screenshot shows the completed banking analytics dashboard built with Metabase.

![Banking Data Stack Metabase Dashboard](https://github.com/user-attachments/assets/3f78cd8f-6a02-407c-859a-5a0d132ff8ef)

The dashboard provides a consolidated view of customer activity, account balances, transaction performance, and banking operations.

## 14. Verify the End-to-End Pipeline

Once all services are running, verify each stage of the pipeline.

| Layer | Verification |
|---|---|
| PostgreSQL | Source tables contain generated data |
| Debezium | Connector is registered and capturing changes |
| Kafka | Topics receive CDC events |
| MinIO | Parquet files are available in the banking-data bucket |
| Airflow | DAG successfully loads files into ClickHouse |
| ClickHouse | Raw and transformed tables are available |
| dbt | Models execute and tests pass |
| Metabase | Database connection works and dashboards display data |

### Example Verification Commands

Check PostgreSQL:

```bash
psql postgresql://banking_user:your_password@localhost:5432/banking_db -c "SELECT COUNT(*) FROM transactions;"
```

Check MinIO:

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
curl "http://localhost:8123/?database=banking_db" -d "SHOW TABLES"
```

Check Airflow logs:

```bash
docker compose logs airflow-scheduler
```

Check Metabase logs:

```bash
docker compose logs metabase
```

## Common Troubleshooting

### Docker Services Fail to Start

Make sure Docker Desktop is running.

Check service status:

```bash
docker compose ps -a
```

View service logs:

```bash
docker compose logs -f
```

### PostgreSQL Connection Errors

- Confirm `.env` variables match the Docker Compose configuration.
- Verify PostgreSQL is running.
- Confirm the database and schema have been created.

### Kafka/Debezium Not Receiving Changes

- Confirm the Debezium connector is active.
- Check Kafka Connect logs:

```bash
docker compose logs -f connect
```

### MinIO Files Not Appearing

- Ensure the Kafka consumer is running.
- Check that the `banking-data` bucket exists.
- Verify MinIO credentials and endpoint configuration.

### Airflow DAG Not Loading

- Confirm the Airflow webserver and scheduler are running.
- Check scheduler logs:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

### dbt Connection Issues

- Confirm ClickHouse is reachable.
- Check the `profiles.yml` configuration.
- Verify the database name, username, and password.

### Metabase Cannot Connect to ClickHouse

- Confirm both containers are running.
- Make sure Metabase and ClickHouse share the same Docker Compose network.
- Use `clickhouse` as the host, not `localhost`.
- Use HTTP port `8123`.
- Verify the ClickHouse database name and credentials.

Check Metabase logs:

```bash
docker compose logs --tail=100 metabase
```

## Recommended Execution Order

For a complete pipeline run, follow this sequence:

1. Start infrastructure with `docker compose up -d`.
2. Apply the PostgreSQL schema.
3. Generate synthetic banking data.
4. Register the Debezium connector.
5. Start the Kafka consumer.
6. Verify Parquet files in MinIO.
7. Trigger the Airflow DAG.
8. Verify ingested data in ClickHouse.
9. Run dbt transformations and tests.
10. Connect Metabase to ClickHouse.
11. Create and publish banking analytics dashboards.

## Future Improvements

The platform can be extended with:

- More advanced dbt transformations and analytical models.
- Additional banking KPIs and dashboards.
- Data quality checks and validation.
- Pipeline monitoring and alerting.
- Kafka and Airflow performance monitoring.
- Improved data security and secrets management.
- Incremental loading and historical data management.
- Automated dashboard refresh and reporting.

## Useful Commands

```bash
# Start all services
docker compose up -d

# Check services
docker compose ps

# View logs
docker compose logs -f
```

# Start Metabase
docker compose up -d metabase

# Run synthetic data generator once
python pipeline/data_generator/data_generator.py --once

# Register Debezium connector
python pipeline/kafka_debezium/kafka_debezium_pipeline.py

# Start Kafka consumer
python pipeline/consumer/kafka_to_minio_pipeline.py

# Repair transaction Parquet values if needed
python repair_transactions.py

# Run dbt transformations
cd transformations/banking_datastack
dbt deps
dbt run
dbt test
```

## Conclusion

This project demonstrates an end-to-end modern data engineering workflow for banking analytics.

It combines CDC-based data ingestion, event streaming, object storage, orchestration, analytical data warehousing, transformation, and business intelligence in one integrated platform.

By combining PostgreSQL, Debezium, Kafka, MinIO, Airflow, ClickHouse, dbt, and Metabase, the project demonstrates how raw banking events can be transformed into structured analytical datasets and presented through interactive dashboards.

