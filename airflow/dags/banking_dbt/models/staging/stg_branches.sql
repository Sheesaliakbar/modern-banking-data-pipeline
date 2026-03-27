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
    -- Unique branch records ensure karne ke liye
    QUALIFY ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY processed_at DESC) = 1
)

select * from renamed