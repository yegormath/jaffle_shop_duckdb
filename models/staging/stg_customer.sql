WITH customer_base AS (
    SELECT
        id,
        name,
        phone,  -- Removing single quotes around phone number
        email,
        address
    FROM {{ ref('raw_customer') }}
)

SELECT
    id,
    name,
    phone,
    email,
    address
FROM customer_base
