with source as (
    select * from {{ source('raw_banking', 'transactions') }}
),
renamed as (
    select
        transaction_id,
        account_id,
        transaction_type,
        amount,
        case 
            when amount > 10000 then TRUE 
            else FALSE 
        end as is_large_transaction, -- Fraud Detection Flag
        transaction_date,
        coalesce(description, 'No Description Provided') as transaction_description, -- Handling NULLs
        balance_after,
        processed_at as loaded_at
    from source
)
select * from renamed