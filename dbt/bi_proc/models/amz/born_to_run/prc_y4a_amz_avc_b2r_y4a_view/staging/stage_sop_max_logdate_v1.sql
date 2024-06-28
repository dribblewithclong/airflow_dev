{{
  config(
    materialized = 'ephemeral'
    )
}}

SELECT 
  country, 
  order_name, 
  asin, 
  MAX(log_date) log_date
FROM {{ ref('sop_incoming_amazon_by_date') }}
WHERE country = 'USA'
group by 1,2,3