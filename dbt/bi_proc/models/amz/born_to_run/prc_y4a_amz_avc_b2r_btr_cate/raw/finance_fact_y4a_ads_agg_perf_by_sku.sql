{{
  config(
    materialized = 'ephemeral'
    )
}}

select 

	report_date AS posting_date,
	channel_lv3 as sales_channel,
	platform_prd_id asin,
	country as country_code, platform,
	
	sum(usd_ads_spend_allocated) as ads_spend,
	sum(usd_ads_gmv_allocated) as ads_gmv

from y4a_finance.fact_y4a_ads_agg_perf_by_sku
where report_date >= current_date - interval '{{ var('born_to_run_days_back') }}' day
and channel_lv3 like 'AVC D%'
group by 1,2,3,4,5