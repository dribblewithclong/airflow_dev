{{
    config(
        materialized='ephemeral'
    )
}}

select 
    a.run_date, 
    b.country, 
    b.vendor_code 
from {{ ref('stage_fact') }} a
cross join {{ ref('stage_fact_v2') }} as b
group by 1,2,3