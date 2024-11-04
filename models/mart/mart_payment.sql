WITH staged_payment AS (
    SELECT
        appointment_id,
        customer_id,
        date_time,
        amount,
        payment_method
    FROM {{ ref('stg_payment') }}  -- Ensure this reference is correct
)

SELECT
    payment_method,
    SUM(amount) AS total_amount
FROM staged_payment
GROUP BY payment_method
ORDER BY total_amount DESC
