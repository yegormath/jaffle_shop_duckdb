WITH raw_payment AS (
    SELECT
        appointment_id,
        customer_id,
        date_time,
        amount,
        payment_method
    FROM {{ ref('payment') }}  -- Ensure that this reference is correct
)

SELECT
    appointment_id,
    customer_id,
    date_time,
    amount,
    LOWER(payment_method) AS payment_method
FROM raw_payment
WHERE payment_method IS NOT NULL
    AND payment_method != 'NA'