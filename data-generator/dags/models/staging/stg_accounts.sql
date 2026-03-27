with source as (
    select * from {{ source('raw_banking', 'accounts') }}
),
renamed as (
    select
        account_id,
        account_number,
        customer_id,
        branch_id,
        account_type,
        balance,
        case 
            when balance < 500 then 'Low Balance'
            when balance between 500 and 5000 then 'Standard'
            else 'High Value'
        end as account_tier,
        status,
        processed_at as loaded_at
    from source
)
select * from renamed