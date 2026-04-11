{{ config(materialized='table') }}

select
  customer_id,
  max(customer_name) as customer_name,
  sum(total_purchase_amount) as lifetime_revenue,
  count(*) as transaction_count,
  max(churn) as churn_flag
from {{ ref('stg_ecommerce_transactions') }}
group by customer_id
