{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
  profile_id, 
  'Sponsored Display' as ads_type, 
  report_date, 
  campaignid, 
  adgroupid, 
  adid,
  sales as sales14d,
  "cost" as spend,
  purchases as order14d,
  unitssold as unitssold14d,
  impressions,
  clicks,
  salespromotedclicks as sales_same_sku_14d,
  purchasespromotedclicks as order_same_sku_14d,
  0 as units_sold_same_sku_14d,
  run_date as de_run_date,
  now() as bi_run_date
from {{ ref('raw_y4a_dwa_amz_ads_spd_adv_prd_v3') }}
where coalesce(sales,0) + coalesce("cost",0) + coalesce(purchases,0) + coalesce(unitssold,0) + coalesce(impressions,0) + coalesce(clicks,0) + 
coalesce(salespromotedclicks,0) + coalesce(purchasespromotedclicks,0) <> 0