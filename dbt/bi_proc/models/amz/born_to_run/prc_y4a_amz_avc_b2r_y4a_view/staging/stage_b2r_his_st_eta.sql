{{
  config(
    materialized = 'ephemeral'
    )
}}

select 

    a.*,
    running_total_ST, 
    running_total_ETA
    
from {{ ref('stage_avc_brn_run_his_v2') }} a
left join {{ ref('stage_b2r_join_eta_st') }} b on  a.run_date = TO_CHAR(b.report_date, 'YYYY-MM-DD') and a.offer_id = b.offer_ST