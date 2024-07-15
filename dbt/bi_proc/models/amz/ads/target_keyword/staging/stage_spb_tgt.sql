{{
  config(
    materialized = 'ephemeral',
    )
}}

SELECT 
	profile_id,
	'Sponsored Brand' AS ads_type,
	report_date::date AS report_date,
	campaignid,
	adgroupid,
	targetingid as targetid,
	keywordid,
	sales as sales14d,   
	"cost" AS spend,
	purchases as order14d,   
	0 as unitssold14d,
	clicks,
	impressions,
	salespromoted as sales14d_same_sku, 
	purchasespromoted as order14d_same_sku, 
	0 as unitssold14d_same_sku,   
	run_date as de_run_date,
	now() as bi_run_date
from {{ ref('raw_y4a_dwa_amz_ads_spb_tgt_v3') }}
WHERE coalesce(sales,0) + coalesce("cost",0) + coalesce(purchases,0) + coalesce(impressions,0) + coalesce(clicks,0) + 
coalesce(salespromoted,0) + coalesce(purchasespromoted,0) <> 0