with

source as (

    select * from {{ ref('stg_payments') }}

),

renamed as (

    select
        -- numbers
        payment_id as payment_id,
        order_id as order_id,
        
        -- text
        payment_method as payment_method,
        
    from source
)

select * from renamed
