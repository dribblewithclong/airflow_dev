{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
	profile_id, 
	report_date, 
	campaignid, 
	adgroupid, 
	adid,
	sales,
	"cost",
	purchases,
	unitssold,
	impressions,
	clicks,
	salespromotedclicks,
	purchasespromotedclicks,
	run_date
from y4a_cdm.y4a_dwa_amz_ads_spd_adv_prd_v3
where report_date >= current_date - interval '{{ var('days_target_keyword_interval') }}' day
and report_date <= current_date
