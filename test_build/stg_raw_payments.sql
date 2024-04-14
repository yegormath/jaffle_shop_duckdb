with

source as (

    select * from {{ ref('raw_payments') }}

),

renamed as (

    select
        -- numbers
        id as id,
        order_id as order_id,
        amount as amount,
        
        -- text
        payment_method as payment_method,
        
    from source
)

select * from renamed
