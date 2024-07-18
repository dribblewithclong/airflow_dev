{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
  profile_id, 
  'Sponsored Brand' as ads_type, 
  range_date::date as report_date, 
  campaignid, 
  adgroupid, 
  replace(adid::varchar, '.0', '') as adid,
  attributedsales14d as sales14d,
  "cost" as spend,
  attributedconversions14d as order14d,
  unitssold14d,
  impressions,
  clicks,
  attributedsales14dsamesku as sales_same_sku_14d,
  attributedconversions14dsamesku as order_same_sku_14d,
  0 as units_sold_same_sku_14d,
  run_date as de_run_date,
  now() as bi_run_date
from {{ ref('raw_y4a_dwa_amz_ads_spb_ads_v2') }}
where coalesce(attributedsales14d,0) + coalesce("cost",0) + coalesce(attributedconversions14d,0) + coalesce(unitssold14d,0) + coalesce(impressions,0) + coalesce(clicks,0) + 
coalesce(attributedsales14dsamesku,0) + coalesce(attributedconversions14dsamesku,0) <> 0