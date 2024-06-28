{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
	report_date, 
	platform_prd_id, 
	quantity_sellout,
	country,
	channel
from y4a_finance.tb_pnl_sellout_consolidate
where 1=1
and report_date >= current_date - 365