{{ config(materialized='table') }}

select
  purchase_date,
  product_category,
  sum(total_purchase_amount) as revenue,
  count(*) as transaction_count
from {{ ref('stg_ecommerce_transactions') }}
group by purchase_date, product_category
