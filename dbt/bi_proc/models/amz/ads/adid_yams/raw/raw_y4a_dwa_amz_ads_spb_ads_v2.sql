{{
  config(
    materialized = 'ephemeral',
    )
}}

select
	profile_id,
	range_date,
	campaignid, 
	adgroupid, 
	adid,
	attributedsales14d,
	"cost",
	attributedconversions14d,
	unitssold14d,
	impressions,
	clicks,
	attributedsales14dsamesku,
	attributedconversions14dsamesku,
	run_date
from y4a_cdm.y4a_dwa_amz_ads_spb_ads_v2
where range_date::date >= current_date - interval '{{ var('days_target_keyword_interval') }}' day
and range_date::date <= current_date