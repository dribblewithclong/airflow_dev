{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
  distinct *,
  coalesce(running_total_st, max_running_total_st) as running_total_st_edited,
  coalesce(running_total_eta, max_running_total_eta) as running_total_eta_edited
from {{ ref('stage_b2r_his_st_eta') }} a
left join {{ ref('stage_b2r_his_st_eta_max_v1') }} b using(offer_id)