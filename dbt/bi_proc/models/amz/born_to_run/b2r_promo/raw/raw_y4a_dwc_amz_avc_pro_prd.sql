{{
    config(
        materialized='ephemeral',
    )
}}

select 

    promotion_id, 
    country, 
    asin, 
    sub_promotion_id,
    run_date

from y4a_cdm.y4a_dwc_amz_avc_pro_prd