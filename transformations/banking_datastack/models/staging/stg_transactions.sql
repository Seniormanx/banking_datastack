{{ config (
            materialized = 'view'
) }}

select distinct

    toUInt64(id) as transaction_id,
    toUInt64(account_id) as account_id,
    lowerUTF8(trim(txn_type)) as txn_type,
    toDecimal64(amount, 2) as amount,
    if(isNull(related_account_id), NULL, toUInt64(related_account_id)) as related_account_id,
    lowerUTF8(trim(status)) as status,
    toDateTime(created_at) as created_at

from
{{ source('banking_db', 'transactions') }}

