{{ config(
            materialized = 'view'
) }}

select
    toUInt64(id) as account_id,
    toUInt64(customer_id) as customer_id,
    lowerUTF8(trim(account_type)) as account_type,
    toDecimal64(balance, 2) as balance,
    trim(currency) trim,
    toDateTime(created_at) as created
from {{ source('banking', 'accounts') }}