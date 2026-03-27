with source as (
    select * from {{ source('raw_banking', 'loans') }}
),

renamed as (
    select
        loan_id,
        customer_id,
        branch_id,
        loan_type,
        principal_amount,
        interest_rate,
        -- Calculating total months of the loan
        datediff('month', start_date, end_date) as loan_duration_months,
        -- Estimated simple interest for the whole period
        round((principal_amount * interest_rate / 100) * (datediff('month', start_date, end_date) / 12), 2) as estimated_total_interest,
        start_date,
        end_date,
        status,
        -- High risk flag if interest is very high or status is overdue
        case 
            when status = 'Defaulted' or interest_rate > 15 then 'High Risk'
            else 'Low Risk'
        end as risk_rating,
        processed_at as loaded_at
    from source
    -- Deduplication logic for loans
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY processed_at DESC) = 1
)

select * from renamed