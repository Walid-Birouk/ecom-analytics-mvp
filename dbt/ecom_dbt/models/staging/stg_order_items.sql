with src as (
  select * from {{ source('raw', 'raw_order_items') }}
)
select
  order_id,
  product_id,
  cast(quantity as int) as quantity,
  cast(unit_price as numeric(12,2)) as unit_price
from src