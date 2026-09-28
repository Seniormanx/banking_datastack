WITH base AS (
    SELECT *
    FROM {{ ref('int_layer') }}
),

total_balance AS (
    SELECT
        account_type,
        account_currency,
        sumIf(account_balance, status = 'completed') AS total_balance_usd
    FROM base
    GROUP BY account_type, account_currency
)

SELECT *
FROM total_balance