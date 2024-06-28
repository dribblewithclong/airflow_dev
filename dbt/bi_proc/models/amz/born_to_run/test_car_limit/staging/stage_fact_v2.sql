{{
    config(
        materialized='ephemeral'
    )
}}

select 
    country, 
    vendor_code 
from {{ ref('stage_fact') }}
group by 1,2