{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
	a.report_date,
	a.profile_id,
	d.sales_team as team,
	d.country,
	e.country_code_3 as country_code,
	b.account_type as channel,
	b.account_name as accname,
	b.account_name ||'_'|| b.country_code as accname_countrycode,
    'Sponsored Products' as advertisingtype,
	a.campaignid,
	a.campaignname,
	a.adgroupid,
	a.adgroupname,
	a.keywordid as targetid,
	a.targeting as targetingexpression,
	cast(null as varchar) as targetingtype,
	cast(null as varchar) as targetingtext,
	a.keywordtype,
	a.keywordid,
	a.keyword,
	a.keywordbid, 
	a.adkeywordstatus as keywordstatus,
	b.currency_code,
	c.to_usd, 
	a.clicks,
	a.impressions,
	a."cost" as spend,
	(a."cost" * c.to_usd) as spend_usd,
	a.sales14d as sales14d,
	(a.sales14d * c.to_usd) as sales14d_usd,
	a.purchases14d as order14d,
	a.unitssoldclicks14d as unitssold14d,
	a.topofsearchimpressionshare 

from {{ ref('api_sps_tgt') }} a 
left join {{ ref('api_profile') }} b on a.profile_id  = b.profile_id
left join {{ ref('analyst_dim_currency') }} c on b.currency_code = c.currency_code and a.report_date::date = c.run_date

left join {{ ref('analyst_ads_account_tracking') }} d on 
	    (case 
			when b.profile_id  = '3596426524437260' then 'A12X25XU8HRIT'
     	 	when b.profile_id  = '1827820861745261' then 'A1EHMBCIDFM0G1' 
     	 	when b.profile_id = '1446911352933607' then 'A22BCT1J1AXL7B'
     	 	when b.profile_id = '1511546149725395' then 'A1VFEBOAIGEIGO'
      		else b.account_id_format 
			end
		) = d.account_id

left join {{ ref('analyst_dim_country') }} e on d.country_code = e.country_code_2