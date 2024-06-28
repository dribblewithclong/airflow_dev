{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
  run_date,
  get_currency_code,
  get_amount,
  row_number() OVER (PARTITION BY get_currency_code, run_date::date ORDER BY run_date DESC) AS rn_last_runtime
from y4a_cdm.all_dwb_exr
