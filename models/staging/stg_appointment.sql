WITH raw_appointment AS (
    SELECT
        id AS appointment_id,
        customer_id,
        service_id,
        employee_id,
        date_time,
        status
    FROM {{ ref('appointment') }}
)

SELECT *
FROM raw_appointment
WHERE appointment_id IS NOT NULL
