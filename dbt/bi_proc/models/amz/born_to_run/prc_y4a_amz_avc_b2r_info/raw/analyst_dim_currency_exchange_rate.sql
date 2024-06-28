{{
  config(
    materialized = 'ephemeral',
    )
}}

select
	currency_code,
	run_date::date as run_date,
	to_usd
from y4a_analyst.dim_currency_exchange_rate
group by 1,2,3