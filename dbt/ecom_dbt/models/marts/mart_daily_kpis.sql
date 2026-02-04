with base as (
  select
    date_trunc('day', order_ts) as day,
    order_id,
    customer_id,
    coalesce(paid_amount, 0) as revenue
  from {{ ref('fct_orders') }}
  where status != 'cancelled'
)
select
  day,
  count(distinct order_id) as orders,
  count(distinct customer_id) as customers,
  sum(revenue) as revenue,
  case when count(distinct order_id) = 0 then 0
       else sum(revenue) / count(distinct order_id)
  end as aov
from base
group by 1
order by 1