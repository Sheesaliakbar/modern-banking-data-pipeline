with source as (
    select * from {{ source('raw_banking', 'employees') }}
),

renamed as (
    select
        employee_id,
        first_name || ' ' || last_name as employee_full_name,
        email,
        phone,
        branch_id,
        position,
        -- Categorizing staff level
        case 
            when position ilike '%Manager%' or position ilike '%Head%' then 'Management'
            when position ilike '%Senior%' then 'Senior Staff'
            else 'Junior/Associate'
        end as staff_level,
        processed_at as loaded_at
    from source
    -- Deduplication logic
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY processed_at DESC) = 1
)

select * from renamed