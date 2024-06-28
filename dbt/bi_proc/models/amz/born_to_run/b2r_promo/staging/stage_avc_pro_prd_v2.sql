{{
    config(
        materialized='ephemeral',
    )
}}

select 

    *

from {{ ref('stage_avc_pro_prd') }} 
where rn = 1
and sub_promotion_id = 'Approved'
