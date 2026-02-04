with orders as (
  select * from {{ ref('stg_orders') }}
),
items as (
  select
    order_id,
    sum(line_amount) as items_amount
  from {{ ref('fct_order_items') }}
  group by 1
),
payments as (
  select * from {{ ref('stg_payments') }}
)
select
  o.order_id,
  o.customer_id,
  o.order_ts,
  o.status,
  i.items_amount,
  p.amount as paid_amount,
  p.payment_method,
  p.paid_ts
from orders o
left join items i on i.order_id = o.order_id
left join payments p on p.order_id = o.order_id