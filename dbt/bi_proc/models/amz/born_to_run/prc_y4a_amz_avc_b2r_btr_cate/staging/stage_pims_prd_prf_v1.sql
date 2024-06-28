{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
    a.*,
    TO_DATE(NPD_month, 'YYYY-MM') as NPD_month_date 
from {{ ref('finance_fact_y4a_gmv_rev_sell_out_by_sku') }} a 
left join {{ ref('finance_dim_pims_product_profile') }} b on a.sku = b.sku  
where b.sellingtype not in ('Combo', 'Multiple') 
and a.NPD_month >= '2023-01'