with customers as (
    select 
        customer_id,
        first_name,
        last_name,
        email
    from {{ ref('stg_customers') }}
),

accounts as (
    select 
        account_id,
        customer_id,
        account_type
    from {{ ref('stg_accounts') }}
),

transactions as (
    select 
        transaction_id,
        account_id,
        txn_type,
        amount,
        status,
        related_account_id
        
    from {{ ref('stg_transactions') }}
)

select
    c.customer_id as customer_id,
    c.first_name as first_name,
    c.last_name as last_name,
    c.email as email,

    a.account_id as account_id,
    a.customer_id as account_customer_id,
    a.account_type as account_type,

    t.transaction_id as transaction_id,
    t.account_id as transaction_account_id,
    t.txn_type as txn_type,
    t.amount as amount, 
    t.related_account_id as related_account_id,
    t.status as status

from customers c
left join accounts a 
    on c.customer_id = a.customer_id
left join transactions t 
    on a.account_id = t.account_id