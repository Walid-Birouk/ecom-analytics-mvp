with src as (
  select * from {{ source('raw', 'raw_products') }}
)
select
  product_id,
  category,
  cast(price as numeric(12,2)) as price
from src
