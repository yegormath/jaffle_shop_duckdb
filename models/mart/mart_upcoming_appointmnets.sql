SELECT
    a.date_time,
    a.status,
    c.name AS customer_name,
    c.phone AS customer_phone,
    s.name AS service_name
FROM {{ ref('stg_appointment') }} a
INNER JOIN {{ ref('stg_customer') }} c ON a.customer_id = c.id
INNER JOIN {{ ref('stg_service') }} s ON a.service_id = s.id
WHERE a.status = 'arranged'
ORDER BY a.date_time
