{{
  config(
    materialized = 'ephemeral'
    )
}}

SELECT 
  asin, 
  order_name poid, 
  salable_date
FROM {{ ref('sop_incoming_amazon_by_date') }}
JOIN {{ ref('stage_sop_max_logdate_v1') }} m USING (country,log_date,order_name,asin)
WHERE qty_load IS NOT NULL