{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
	report_date::date as report_date,
	profile_id,
	campaignid::text as campaignid,
	campaignname,
	adgroupid::text as adgroupid,
	adgroupname,
	targetingid,
	targetingexpression,

	targetingtext,
	adkeywordstatus,

	clicks,
	impressions,
	"cost",
	sales,
	purchases,
	unitssold,
	salespromotedclicks,
	purchasespromotedclicks,
	run_date
from y4a_cdm.y4a_dwa_amz_ads_spd_tgt_v3
where report_date >= current_date - interval '{{ var('days_target_keyword_interval') }}' day
and report_date <= current_date
