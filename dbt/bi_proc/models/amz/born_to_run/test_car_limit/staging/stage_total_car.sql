{{
    config(
        materialized='ephemeral'
    )
}}

select 

    channel, 
    market country,
    vendor_code,
    sum(cast(replace(car_limit, ',', '') as numeric)) as car_limit

from {{ ref('analyst_tb_gs_b2r_di_total_car_by_market') }} 
group by 1,2,3