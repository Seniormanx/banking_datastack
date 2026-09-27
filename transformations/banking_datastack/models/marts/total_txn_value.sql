
WITH base AS (
    SELECT *
    FROM {{ ref('int_layer') }}
),

txn_value AS (
    SELECT
        txn_type,
        countIf(status = 'completed') AS completed_txn_count,
        countIf(status = 'failed') AS failed_txn_count,
        sumIf(amount, status = 'completed') AS completed_txn_value,
        sumIf(amount, status = 'failed') AS failed_txn_value
    FROM base
    GROUP BY txn_type
)

SELECT *
FROM txn_value

