{{
    config(
        materialized='ephemeral',
    )
}}

select 
    promotion_id, 
    country, 
    "type", 
    "start_date"::date as "start_date", 
    end_date::date as end_date, 
    "status", 
    sub_promotion_id,
    run_date
from y4a_cdm.y4a_dwc_amz_avc_pro_inf