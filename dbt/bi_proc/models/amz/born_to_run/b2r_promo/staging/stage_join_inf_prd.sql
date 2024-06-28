{{
    config(
        materialized='ephemeral',
    )
}}

select 
    a.*, 
    b.asin,
case 
    when start_date <= current_date 
    and current_date <= end_date then true 
    else false 
end as is_currently_in_promo

from {{ ref('stage_avc_pro_inf_v2') }} a
left join {{ ref('stage_avc_pro_prd_v2') }} b using(promotion_id, country)