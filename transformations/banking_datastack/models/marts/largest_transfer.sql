
WITH base AS (
    SELECT *
    FROM {{ ref('int_layer') }}
),

largest_transfer AS (
    SELECT
        concat(first_name, ' ', last_name) AS account_name,
        email,
        account_type,
        sumIf(amount, txn_type = 'transfer' AND status = 'completed') AS total_transfers
    FROM base
    GROUP BY
       first_name,
       last_name,
       email,
       account_type
)

SELECT *
FROM largest_transfer
ORDER BY total_transfers DESC
limit 10

