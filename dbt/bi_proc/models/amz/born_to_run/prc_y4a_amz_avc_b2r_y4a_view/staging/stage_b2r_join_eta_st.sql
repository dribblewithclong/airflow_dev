{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
    a.*, 
    running_total_ETA
from {{ ref('stage_b2r_st') }} a

left join {{ ref('stage_b2r_eta') }} b on a.report_date = b.report_date and a.offer_ST = b.offer_ETA and a.quantity_sellout = b.quantity_sellout

where coalesce(offer_st, offer_eta) is not null 
and a.report_date >= '2023-11-25'