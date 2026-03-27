with source as (
    select * from {{ source('raw_banking', 'branches') }}
),

renamed as (
    select
        branch_id,
        upper(branch_name) as branch_name, -- Standardizing to uppercase
        trim(address) as street_address,
        city,
        zip_code,
        -- Creating a full location string for reporting
        branch_name || ' (' || city || ', ' || zip_code || ')' as branch_full_location,
        processed_at as loaded_at
    from source
)

select * from renamed