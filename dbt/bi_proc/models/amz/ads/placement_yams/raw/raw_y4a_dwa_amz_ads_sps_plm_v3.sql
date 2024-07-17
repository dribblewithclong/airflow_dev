{{
  config(
    materialized = 'ephemeral',
    )
}}

select  
	report_date,
	profile_id,
    campaignid,
    placementclassification,
	clicks,
	purchases14d,
	sales14d,
	unitssoldclicks14d,
	"cost",
	impressions,
	attributedsalessamesku14d,
	purchasessamesku14d,
	unitssoldsamesku14d,
	run_date

from y4a_cdm.y4a_dwa_amz_ads_sps_plm_v3
where report_date >= current_date - interval '{{ var('days_target_keyword_interval') }}' day
and report_date <= current_date