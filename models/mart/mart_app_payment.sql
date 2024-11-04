WITH appointments AS (
    SELECT
        appointment_id,
        customer_id,
        service_id,
        employee_id,
        date_time,
        status
    FROM {{ ref('stg_appointment') }}
),

payments AS (
    SELECT
        appointment_id,
        customer_id,
        date_time,
        amount,
        LOWER(payment_method) AS payment_method
    FROM {{ ref('stg_payment') }}
),

aggregated_stats AS (
    SELECT
        a.appointment_id,
        a.customer_id,
        a.service_id,
        a.employee_id,
        a.date_time,
        a.status,
        COUNT(p.amount) AS payment_count,                       -- Count of payments per appointment
        SUM(p.amount) AS total_payment_amount,                  -- Total payment amount per appointment
        AVG(p.amount) AS average_payment_amount,                -- Average payment amount per appointment
        STRING_AGG(DISTINCT p.payment_method, ', ') AS payment_methods  -- Concatenate unique payment methods
    FROM appointments a
    LEFT JOIN payments p ON a.appointment_id = p.appointment_id
    GROUP BY
        a.appointment_id,
        a.customer_id,
        a.service_id,
        a.employee_id,
        a.date_time,
        a.status
)

SELECT
    COUNT(*) AS total_appointments,                           -- Total number of appointments
    SUM(payment_count) AS total_payments,                    -- Total number of payments
    SUM(total_payment_amount) AS grand_total_payment,        -- Grand total payment amount
    AVG(average_payment_amount) AS average_payment_per_appointment,  -- Average payment per appointment
    MIN(total_payment_amount) AS min_payment_per_appointment,  -- Minimum payment amount
    MAX(total_payment_amount) AS max_payment_per_appointment,  -- Maximum payment amount
    STRING_AGG(DISTINCT payment_methods, '; ') AS all_payment_methods  -- All unique payment methods used
FROM aggregated_stats