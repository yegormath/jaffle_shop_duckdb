WITH stg_products AS (
    SELECT
        id AS product_id,
        name,
        description,
        quantity,
        cost
    FROM {{ ref('product') }}
)

SELECT *
FROM stg_products
WHERE product_id IS NOT NULL