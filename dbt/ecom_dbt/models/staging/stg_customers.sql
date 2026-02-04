with src as (
  select * from {{ source('raw', 'raw_customers') }}
)
select
  customer_id,
  cast(created_at as timestamptz) as created_at,
  country,
  city
from src