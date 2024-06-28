{{
  config(
    materialized = 'ephemeral',
    )
}}


select
    date(a.run_date) as run_date,
    a.currency_code,
    a.exchange_value,
    a.exchange_value as to_vnd,
    a.exchange_value / b.exchange_value as to_usd
from y4a_cdm.dim_exc_rate a
left join y4a_cdm.dim_exc_rate b on b.run_date = a.run_date and b.currency_code = 'USD' and b.run_date is not null
where a.run_date is not null