{{ config(
    materialized='table'
) }}

SELECT
    customer_id,

    -- total paid online by customer
    {{ conditional_sum('amount', 'payment_method', 'online') }} AS total_online_payments,

    -- total paid in store by customer
    {{ conditional_sum('amount', 'payment_method', 'salon cash') }} +
    {{ conditional_sum('amount', 'payment_method', 'salon card') }} AS total_instore_payments

FROM {{ ref('stg_payment') }}
GROUP BY customer_id
