with customers as (
    select * from {{ ref('stg_customers') }}
),

loans as (
    select * from {{ ref('stg_loans') }}
),

customer_loans as (
    select
        c.customer_id,
        c.full_name,
        count(l.loan_id) as active_loans,
        sum(l.principal_amount) as total_loan_amount,
        max(l.risk_rating) as overall_risk_status
    from customers c
    left join loans l on c.customer_id = l.customer_id
    group by 1, 2
)

select * from customer_loans