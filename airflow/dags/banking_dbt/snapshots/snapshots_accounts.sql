{% snapshot snapshot_accounts %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='timestamp',
      updated_at='processed_at',
      invalidate_hard_deletes=True,
    )
}}

-- Hum QUALIFY use kar rahe hain duplicates ko filter karne ke liye
select 
    account_id,
    account_number,
    customer_id,
    branch_id,
    account_type,
    cast(balance as decimal(15,2)) as balance,
    status,
    processed_at
from {{ source('raw_banking', 'accounts') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY processed_at DESC) = 1

{% endsnapshot %}