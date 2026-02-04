with src as (
  select * from {{ source('raw', 'raw_orders') }}
)
select
  order_id,
  customer_id,
  cast(order_ts as timestamptz) as order_ts,
  status
from src