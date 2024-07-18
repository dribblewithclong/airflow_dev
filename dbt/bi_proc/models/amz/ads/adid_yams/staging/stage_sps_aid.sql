{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
profile_id, 
'Sponsored Products' as ads_type, 
report_date as report_date, 
campaignid, 
adgroupid, 
adid,
sales14d,
"cost" as spend,
purchases14d as order14d,
unitssoldclicks14d as unitssold14d,
impressions,
clicks,
attributedsalessamesku14d as sales_same_sku_14d,
purchasessamesku14d as order_same_sku_14d,
unitssoldsamesku14d as units_sold_same_sku_14d,
run_date as de_run_date,
now() as bi_run_date
from {{ ref('raw_y4a_dwa_amz_ads_sps_adv_prd_v3') }}
WHERE coalesce(sales14d,0) + coalesce("cost",0) + coalesce(purchases14d,0) + coalesce(unitssoldclicks14d,0) + coalesce(impressions,0) + coalesce(clicks,0) + 
coalesce(attributedsalessamesku14d,0) + coalesce(purchasessamesku14d,0) + coalesce(unitssoldsamesku14d,0) <> 0