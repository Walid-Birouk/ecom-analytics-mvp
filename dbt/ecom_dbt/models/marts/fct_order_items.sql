select
  order_id,
  product_id,
  quantity,
  unit_price,
  (quantity * unit_price) as line_amount
from {{ ref('stg_order_items') }}
