with

source as (

    select * from {{ ref('customers') }}

),

renamed as (

    select
        -- datetimes
        first_order as first_order,
        most_recent_order as most_recent_order,
        
        -- numbers
        customer_id as customer_id,
        number_of_orders as number_of_orders,
        
        -- text
        first_name as first_name,
        last_name as last_name,
        
    from source
)

select * from renamed
