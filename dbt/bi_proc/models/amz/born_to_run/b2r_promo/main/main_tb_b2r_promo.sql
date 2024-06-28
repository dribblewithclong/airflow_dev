{{
    config(
        materialized='table'
    )
}}

select 

    asin, 
    country,
    STRING_AGG(promotion_id,', ' ORDER BY start_date ASC)  as promo_active,
    STRING_AGG(type,', ' ORDER BY start_date ASC)  as type_active,
    STRING_AGG(to_char(start_date, 'MM-DD-YYYY'),', ' ORDER BY start_date ASC)  as start_date_active,
    STRING_AGG(to_char(end_date, 'MM-DD-YYYY'),', ' ORDER BY start_date ASC)  as end_date_active

from {{ ref('stage_join_inf_prd') }}
where is_currently_in_promo = true
group by 1,2