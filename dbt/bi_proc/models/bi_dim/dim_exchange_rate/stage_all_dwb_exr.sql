{{
  config(
    materialized = 'ephemeral'
    )
}}


select 
    run_date,
    get_currency_code,
    get_amount
from {{ ref('raw_all_dwb_exr') }}
where rn_last_runtime = 1
 