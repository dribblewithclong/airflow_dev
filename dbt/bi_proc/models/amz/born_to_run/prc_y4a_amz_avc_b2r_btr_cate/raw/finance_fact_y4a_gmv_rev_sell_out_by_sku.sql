{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
    sku, 
    min(to_char(report_date,'YYYY-MM')) as NPD_month 
from y4a_finance.fact_y4a_gmv_rev_sell_out_by_sku  
where shipped_gmv_usd > 0 
group by 1 