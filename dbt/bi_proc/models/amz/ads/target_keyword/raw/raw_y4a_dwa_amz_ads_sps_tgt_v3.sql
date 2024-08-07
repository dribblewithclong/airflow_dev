{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
	report_date::date as report_date,
	profile_id,
	campaignid,
	campaignname,
	adgroupid,
	adgroupname,
	keywordid,
	targeting,
	keywordtype,
	keyword,
	keywordbid, 
	adkeywordstatus,
	clicks,
	impressions,
	"cost",
	sales14d,
	purchases14d,
	unitssoldclicks14d,
	topofsearchimpressionshare,

	attributedsalessamesku14d,
	purchasessamesku14d,
	unitssoldsamesku14d,
	run_date

from y4a_cdm.y4a_dwa_amz_ads_sps_tgt_v3
where report_date >= current_date - interval '{{ var('days_target_keyword_interval') }}' day
and report_date <= current_date