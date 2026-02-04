with src as (
  select * from {{ source('raw', 'raw_payments') }}
)
select
  order_id,
  payment_method,
  cast(amount as numeric(12,2)) as amount,
  cast(paid_ts as timestamptz) as paid_ts
from src
