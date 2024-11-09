WITH service_base AS (
    SELECT
        id,
        name,
        description,
        duration,
        price
    FROM {{ ref('service') }}
)

SELECT
    id,
    name,
    description,
    duration,
    price
FROM service_base