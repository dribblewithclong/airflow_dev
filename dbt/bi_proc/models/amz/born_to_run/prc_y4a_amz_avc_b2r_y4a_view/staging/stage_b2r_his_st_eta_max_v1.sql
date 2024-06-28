{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
  offer_id,
  max(running_total_st) as max_running_total_st,   
  max(running_total_eta) as max_running_total_eta
from {{ ref('stage_b2r_his_st_eta') }}
group by 1