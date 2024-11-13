WITH raw_payment AS (
    SELECT
        appointment_id,
        customer_id,
        date_time,
        amount,
        LOWER(payment_method) AS payment_method,
        (CONCAT(appointment_id, '_', customer_id)) AS payment_id
    FROM {{ ref('payment') }}
    WHERE payment_method IS NOT NULL
)

SELECT *
FROM raw_payment
