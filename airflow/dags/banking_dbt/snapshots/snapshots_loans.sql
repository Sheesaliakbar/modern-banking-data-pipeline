{% snapshot snapshot_loans %}

{{
    config(
      target_schema='snapshots',
      unique_key='loan_id',
      strategy='timestamp',
      updated_at='processed_at',
      invalidate_hard_deletes=True,
    )
}}

select 
    loan_id,
    customer_id,
    loan_type,
    cast(principal_amount as decimal(15,2)) as principal_amount,
    cast(interest_rate as decimal(5,2)) as interest_rate,
    start_date,
    end_date,
    status,
    processed_at
from {{ source('raw_banking', 'loans') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY processed_at DESC) = 1
{% endsnapshot %}