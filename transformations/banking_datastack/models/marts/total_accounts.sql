
WITH base AS (

    SELECT *
    FROM {{ ref('int_layer') }}

),

account as (

    SELECT
        COUNT(DISTINCT account_id) as total_accounts

    FROM base

)

SELECT *
FROM account

