{{ config(
            materialized = 'view'
)}}

select 
     toUInt64(id) AS customer_id,
    trim(first_name) AS first_name,
    trim(last_name) AS last_name,
    lowerUTF8(trim(email)) AS email,
    toDateTime(created_at) as created_at
from {{ source('banking', 'customers') }}
