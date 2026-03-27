with source as (
    select * from {{ source('raw_banking', 'customers') }}
),
renamed as (
    select
        customer_id,
        first_name || ' ' || last_name as full_name, -- Combining names
        email,
        split_part(email, '@', 2) as email_domain, -- Extracting domain for marketing analytics
        phone,
        date_of_birth,
        floor(datediff('year', date_of_birth, current_date())) as age, -- Calculating current age
        processed_at as loaded_at
    from source
)
select * from renamed