
WITH base AS (

    SELECT *
    FROM {{ ref('int_layer') }}

),

customer AS (

    SELECT
        COUNT(DISTINCT customer_id) as total_customer
    FROM base

)

SELECT *
FROM customer

