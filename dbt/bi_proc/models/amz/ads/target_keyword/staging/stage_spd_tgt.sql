{{
  config(
    materialized = 'ephemeral',
    )
}}

SELECT 
	profile_id,
	'Sponsored Display' AS ads_type,
	report_date::date AS report_date,
	campaignid::text,
	adgroupid::text,
	targetingid::text,
	targetingid AS keywordid,
	sales as sales14d,
	"cost" AS spend,
	purchases as order14d,   
	unitssold as unitssold14d,   
	clicks,
	impressions,
	salespromotedclicks as sales14d_same_sku,                                                                                       
	purchasespromotedclicks as order14d_same_sku,    
	0 as unitssold14d_same_sku,
	run_date as de_run_date,
	now() as bi_run_date
from {{ ref('raw_y4a_dwa_amz_ads_spd_tgt_v3') }}
where coalesce(sales,0) + coalesce("cost",0) + coalesce(purchases,0) + coalesce(unitssold,0) + coalesce(impressions,0) + coalesce(clicks,0) + 
coalesce(salespromotedclicks,0) + coalesce(purchasespromotedclicks,0) <> 0