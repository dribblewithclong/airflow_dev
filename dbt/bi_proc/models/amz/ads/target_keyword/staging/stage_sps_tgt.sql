{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
	profile_id,
	'Sponsored Products' AS ads_type,
	report_date::date,
	campaignid,
	adgroupid,
	keywordid AS targetid,
	keywordid,
	sales14d,       
	"cost" AS spend,
	purchases14d as order14d,    
	unitssoldclicks14d as unitssold14d,
	clicks,
	impressions,
	attributedsalessamesku14d as sales14d_same_sku, 
	purchasessamesku14d  as order14d_same_sku,                      
	unitssoldsamesku14d  as unitssold14d_same_sku,
	run_date as de_run_date,
	now() as bi_run_date

from {{ ref('raw_y4a_dwa_amz_ads_sps_tgt_v3') }} 
where coalesce(sales14d,0) + coalesce("cost",0) + coalesce(purchases14d,0) + coalesce(unitssoldclicks14d,0) + coalesce(impressions,0) + coalesce(clicks,0) + 
coalesce(attributedsalessamesku14d,0) + coalesce(purchasessamesku14d,0) + coalesce(unitssoldsamesku14d,0) <> 0
