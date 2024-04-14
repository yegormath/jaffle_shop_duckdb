with

source as (

    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        -- numbers
        id as id,
        
        -- text
        first_name as first_name,
        last_name as last_name,
        
    from source
)

select * from renamed
