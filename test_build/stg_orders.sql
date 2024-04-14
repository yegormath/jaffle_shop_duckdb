with

source as (

    select * from {{ ref('orders') }}

),

renamed as (

    select
        -- datetimes
        order_date as order_date,
        
        -- numbers
        order_id as order_id,
        customer_id as customer_id,
        
        -- text
        status as status,
        
    from source
)

select * from renamed
