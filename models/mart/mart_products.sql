WITH stg_products AS (
    SELECT
        product_id,
        name,
        description,
        quantity,
        cost
    FROM {{ ref('stg_products') }}
),

-- Define cost ranges and rank products within each range by quantity
product_cost_analysis AS (
    SELECT
        product_id,
        name,
        description,
        quantity,
        cost,

        -- Define cost ranges
        CASE
            WHEN cost < 10 THEN 'Low'
            WHEN cost BETWEEN 10 AND 50 THEN 'Medium'
            WHEN cost BETWEEN 50 AND 100 THEN 'High'
            ELSE 'Premium'
        END AS cost_range,

        -- Rank products within each cost range by quantity (descending)
        RANK() OVER (PARTITION BY
            CASE
                WHEN cost < 10 THEN 'Low'
                WHEN cost BETWEEN 10 AND 50 THEN 'Medium'
                WHEN cost BETWEEN 50 AND 100 THEN 'High'
                ELSE 'Premium'
            END
            ORDER BY quantity DESC) AS quantity_rank
    FROM stg_products
)

SELECT
    cost_range,
    product_id,
    name,
    description,
    quantity,
    cost,
    quantity_rank,
    AVG(quantity) OVER (PARTITION BY cost_range) AS avg_quantity_in_range,  -- Average quantity within each cost range
    COUNT(product_id) OVER (PARTITION BY cost_range) AS total_products_in_range  -- Total products within each range
FROM product_cost_analysis
ORDER BY cost_range, quantity_rank