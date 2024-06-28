{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
	report_date, 
	platform_prd_id, 
	'AVC DI' as channel, 
	SUM(quantity_sellout) as quantity_sellout
from {{ ref('finance_tb_pnl_sellout_consolidate') }}
where 1=1
and country = 'USA'
and channel like 'AVC%'
group by 1,2,3