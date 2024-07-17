{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
	report_date,
	profile_id,
    'Sponsored Brand' as ads_type,
    campaignid,
    placementclassification as placement,
	clicks,
	purchases as order14d,
	sales as sales14d,
	unitssoldclicks as unitssold14d,
	"cost" as spend,
	impressions,
	salespromoted as sales14d_same_sku,
	purchasespromoted as order14d_same_sku,
	0 as unitssold14d_same_sku,
	run_date as de_run_date,
	now() as bi_run_time
from {{ ref('raw_y4a_dwa_amz_ads_spb_plm_v3') }}
where coalesce(sales,0) + coalesce("cost",0) + coalesce(purchases,0) + coalesce(unitssoldclicks,0) + coalesce(impressions,0) + coalesce(clicks,0) + 
coalesce(salespromoted,0) + coalesce(purchasespromoted,0) <> 0