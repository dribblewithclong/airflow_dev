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
	targetingid,
	targetingexpression,
	targetingtype,
	targetingtext,
	keywordtype,
	keywordid,
	keywordtext,
	keywordbid,
	adkeywordstatus,

	clicks,
	impressions,
	"cost",
	sales,
	purchases,

	topofsearchimpressionshare
 
{# from y4a_temp.f_y4a_dwa_amz_ads_spb_tgt_v3 #}
from y4a_cdm.y4a_dwa_amz_ads_spb_tgt_v3
where report_date >= current_date - interval '{{ var('days_interval') }}' day
and report_date <= current_date