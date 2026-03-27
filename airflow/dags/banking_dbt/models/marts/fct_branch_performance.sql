with branches as (
    select * from {{ ref('stg_branches') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final_join as (
    select
        b.branch_id,
        b.branch_name,
        b.city,
        count(distinct c.customer_id) as total_customers,
        count(distinct a.account_id) as total_accounts,
        sum(a.balance) as total_branch_balance,
        round(avg(a.balance), 2) as avg_account_balance
    from branches b
    left join accounts a on b.branch_id = a.branch_id
    left join customers c on a.customer_id = c.customer_id
    group by 1, 2, 3
)

select * from final_join