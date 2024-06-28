{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
    *
from {{ ref('stage_avc_brn_run_his_v1') }}
where rn = 1