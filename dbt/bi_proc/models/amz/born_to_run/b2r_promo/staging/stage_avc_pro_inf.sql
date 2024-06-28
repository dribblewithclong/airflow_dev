{{
    config(
        materialized='ephemeral',
    )
}}

select

    *,
    row_number() over(partition by promotion_id, country order by run_date desc) rn
    
from {{ ref('raw_y4a_dwc_amz_avc_pro_inf') }}