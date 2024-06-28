{{
  config(
    materialized = 'ephemeral'
    )
}}

SELECT 
	country,
	order_name,
	asin,
	salable_date,
	log_date,
	qty_load
FROM y4a_sop_ops.incoming_amazon_by_date