
CREATE DATABASE IF NOT EXISTS banking_db;

-- CUSTOMERS
CREATE TABLE IF NOT EXISTS banking_db.customers
(
    id UInt64,
    first_name String,
    last_name String,
    email String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY id;


-- ACCOUNTS
CREATE TABLE IF NOT EXISTS banking_db.accounts
(
    id UInt64,
    customer_id UInt64,
    account_type LowCardinality(String),
    balance Decimal(18, 2) DEFAULT 0,
    currency FixedString(3) DEFAULT 'USD',
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (customer_id, id);


-- TRANSACTIONS
CREATE TABLE IF NOT EXISTS banking_db.transactions
(
    id UInt64,
    account_id UInt64,
    txn_type LowCardinality(String),
    amount Decimal(18, 2),
    related_account_id Nullable(UInt64),
    status LowCardinality(String) DEFAULT 'COMPLETED',
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (account_id, created_at, id);
