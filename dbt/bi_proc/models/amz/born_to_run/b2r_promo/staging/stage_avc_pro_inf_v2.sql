{{
    config(
        materialized='ephemeral',
    )
}}

select 

    *

from {{ ref('stage_avc_pro_inf') }} 
where rn = 1
and status not in ('Approved but did not run', 'Canceled')
and sub_promotion_id = 'Approved'