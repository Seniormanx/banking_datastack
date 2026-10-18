WITH base AS (

    SELECT *
    FROM {{ ref('int_layer') }}
    where txn_type is not null

),

txn_status AS (

    SELECT
        txn_type,
        countIf(status = 'completed') AS completed_txn,
        countIf(status = 'failed') AS failed_txn
    FROM base
    GROUP BY txn_type

)

SELECT *
FROM txn_status