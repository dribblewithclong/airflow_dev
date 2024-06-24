{{
  config(
    materialized = 'ephemeral'
    )
}}

with union_all as
(
    select * from {{ ref('stage_spb_tgt') }}
    union all
    select * from {{ ref('stage_sps_tgt') }}
    union all
    select * from {{ ref('stage_spd_tgt') }}
)

SELECT 
	report_date,
	profile_id,
	team,
	country,
	country_code,
	channel,
	accname,
	accname_countrycode,
	advertisingtype,
	campaignid,
	campaignname,
	adgroupid,
	adgroupname,
	targetid,
	targetingexpression,
	targetingtype,
	targetingtext,
	keywordtype,
	keywordid,
	keyword,
	keywordbid, 
	keywordstatus,
	clicks,
	impressions,
	to_usd,
	spend,
	spend_usd,
	sales14d,
	sales14d_usd,
	spend_usd/ NULLIF(clicks,0) AS cpc_usd,
	sales14d/ NULLIF(spend_usd,0) AS roas,
	spend_usd/ NULLIF(sales14d,0) AS acos,
	clicks/ NULLIF(impressions,0) AS ctr,
	unitssold14d,
	order14d,
	topofsearchimpressionshare
FROM union_all
where coalesce(sales14d,0) + coalesce(spend,0) + coalesce(order14d,0) + coalesce(unitssold14d,0) + coalesce(impressions,0) + coalesce(clicks,0) <> 0