select
  customer_id,
  created_at,
  country,
  city
from {{ ref('stg_customers') }}
