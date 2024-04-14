with

source as (

    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        -- datetimes
        order_date as order_date,
        
        -- numbers
        id as id,
        user_id as user_id,
        
        -- text
        status as status,
        
    from source
)

select * from renamed
