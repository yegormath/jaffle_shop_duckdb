with

source as (

    select * from {{ ref('stg_customers') }}

),

renamed as (

    select
        -- numbers
        customer_id as customer_id,
        
        -- text
        first_name as first_name,
        last_name as last_name,
        
    from source
)

select * from renamed
