{% snapshot snapshot_customers %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='processed_at',
      invalidate_hard_deletes=True,
    )
}}

select 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    processed_at
from {{ source('raw_banking', 'customers') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY processed_at DESC) = 1

{% endsnapshot %}