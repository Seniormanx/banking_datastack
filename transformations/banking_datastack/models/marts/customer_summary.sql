
WITH base AS (
    SELECT *
    FROM {{ ref('int_layer') }}
),

customer_summary AS (
    SELECT
        customer_id,
        concat(first_name, ' ', last_name) AS customer_name,
        email,
        account_type,
        account_balance,
        count(account_id) as num_of_account,
        sumIf(
            amount,
            txn_type = 'withdrawal'
            AND status = 'completed'
        ) AS total_withdrawal,

        sumIf(
            amount,
            txn_type = 'transfer'
            AND status = 'completed'
        ) AS total_transfer,

        sumIf(
            amount,
            txn_type = 'deposit'
            AND status = 'completed'
        ) AS total_deposit,

        sumIf(
            amount,
            status = 'completed'
        ) AS total_txn_value

    FROM base

    GROUP BY
        customer_id,
        first_name,
        last_name,
        email,
        account_type,
        account_balance
)

SELECT *
FROM customer_summary
