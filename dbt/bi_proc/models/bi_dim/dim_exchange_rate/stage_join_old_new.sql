{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
    coalesce(a.run_date::date, b.run_date::date) as run_date,
    coalesce(a.get_currency_code, b.currency_code) as currency_code,
    coalesce(1::double precision / get_amount, to_usd) as to_usd,
    coalesce(get_amount, to_vnd) as to_vnd,
    now() as bi_run_time
from {{ ref('raw_dim_exc_rate') }} b
full join {{ ref('raw_all_dwb_exr') }} a  on a.get_currency_code = b.currency_code and a.run_date::date = b.run_date::date