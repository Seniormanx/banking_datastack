
WITH base AS (
    SELECT *
    FROM {{ ref('int_layer') }}
),

largest_deposit AS (
    SELECT
        concat(first_name, ' ', last_name) AS account_name,
        email,
        account_type,
        sumIf(amount, txn_type = 'deposit' AND status = 'completed') AS total_deposits
    FROM base
    GROUP BY
       first_name,
       last_name,
       email,
       account_type
)

SELECT *
FROM largest_deposit
ORDER BY total_deposits DESC
limit 10

