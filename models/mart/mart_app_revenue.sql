{{ config(
    materialized='table'
) }}

WITH appointment_data AS (
    SELECT
        appointment_id,
        date_time,
        status,
        customer_id,
        service_id,
        employee_id
    FROM {{ ref('stg_appointment') }}
),

customer_data AS (
    SELECT
        id AS customer_id,
        name AS customer_name,
        phone AS customer_phone,
        email AS customer_email,
        address AS customer_address
    FROM {{ ref('stg_customer') }}
),

payment_data AS (
    SELECT
        appointment_id,
        date_time AS payment_date,
        amount AS payment_amount,
        LOWER(payment_method) AS payment_method
    FROM {{ ref('stg_payment') }}
    WHERE payment_method IS NOT NULL AND payment_method != 'NA'
),

product_data AS (
    SELECT
        product_id,
        name AS product_name,
        description AS product_description,
        quantity,
        cost AS product_cost
    FROM {{ ref('stg_products') }}
),

service_data AS (
    SELECT
        id AS service_id,
        name AS service_name,
        description AS service_description,
        duration,
        price AS service_price
    FROM {{ ref('stg_service') }}
),

appointment_revenue AS (
    SELECT
        a.appointment_id,
        a.date_time,
        a.status,
        c.customer_name,
        c.customer_phone,
        s.service_name,
        s.service_price,
        p.payment_amount,
        p.payment_method,

        -- Calculate product revenue if products are associated with appointments
        COALESCE(pr.quantity * pr.product_cost, 0) AS total_product_cost,

        -- Calculate total revenue per appointment (services + products)
        s.service_price + COALESCE(pr.quantity * pr.product_cost, 0) AS total_appointment_revenue
    FROM appointment_data a
    LEFT JOIN customer_data c ON a.customer_id = c.customer_id
    LEFT JOIN service_data s ON a.service_id = s.service_id
    LEFT JOIN payment_data p ON a.appointment_id = p.appointment_id
    LEFT JOIN product_data pr ON a.appointment_id = pr.product_id  -- Assuming products can be linked to appointments
)

SELECT
    appointment_id,
    date_time,
    status,
    customer_name,
    customer_phone,
    service_name,
    service_price,
    payment_amount,
    payment_method,
    total_product_cost,
    total_appointment_revenue
FROM appointment_revenue
WHERE status = 'arranged'
ORDER BY date_time
