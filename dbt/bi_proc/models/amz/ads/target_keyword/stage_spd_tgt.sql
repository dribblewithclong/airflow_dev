{{
  config(
    materialized = 'ephemeral',
    )
}}

SELECT 
	a.report_date,
	a.profile_id,
	d.sales_team as team,
	d.country,
	e.country_code_3 as country_code,
	b.account_type as channel,
	b.account_name as accname,
	b.account_name ||'_'|| b.country_code as accname_countrycode,
	'Sponsored Display' as advertisingtype,
	a.campaignid,
	a.campaignname,
	a.adgroupid,
	a.adgroupname as adgroupname,
	a.targetingid as targetid,  
	a.targetingexpression,
	cast(null as varchar) as targetingtype,
	a.targetingtext, 
	cast(null as varchar) as keywordtype,
	cast(null as varchar) as keywordid, 
	cast(null as varchar) as keyword,
	cast(null as numeric) as keywordbid,
	a.adkeywordstatus as keywordstatus,  
	b.currency_code,
	c.to_usd, 
	a.clicks,
	a.impressions,
	a."cost" as spend,
	(a."cost" * c.to_usd) as spend_usd,
	a.sales as sales14d,
	(a.sales * c.to_usd) as sales14d_usd,
	a.purchases as order14d,
	a.unitssold as unitssold14d,  
	0 as topofsearchimpressionshare
from {{ ref('raw_y4a_dwa_amz_ads_spd_tgt_v3') }} a 
left join {{ ref('raw_y4a_dwa_amz_ads_prf') }} b on a.profile_id  = b.profile_id
left join {{ ref('analyst_dim_currency_exchange_rate') }} c on b.currency_code = c.currency_code and a.report_date::date = c.run_date

left join {{ ref('analyst_list_ads_account_tracking') }} d on 
	    (case 
			when b.profile_id  = '3596426524437260' then 'A12X25XU8HRIT'
     	 	when b.profile_id  = '1827820861745261' then 'A1EHMBCIDFM0G1' 
     	 	when b.profile_id = '1446911352933607' then 'A22BCT1J1AXL7B'
     	 	when b.profile_id = '1511546149725395' then 'A1VFEBOAIGEIGO'
      		else b.account_id_format 
			end
		) = d.account_id

left join {{ ref('analyst_dim_country') }} e on d.country_code = e.country_code_2