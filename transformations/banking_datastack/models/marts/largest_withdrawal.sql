
WITH base AS (
    SELECT *
    FROM {{ ref('int_layer') }}
),

largest_withdrawal AS (
    SELECT
        concat(first_name, ' ', last_name) AS account_name,
        email,
        account_type,
        sumIf(amount, txn_type = 'withdrawal' AND status = 'completed') AS total_withdrawal
    FROM base
    GROUP BY
       first_name,
       last_name,
       email,
       account_type
)

SELECT *
FROM largest_withdrawal
ORDER BY total_withdrawal DESC
limit 10

