{{
    config(
        materialized='ephemeral'
    )
}}

select 

    *

from y4a_analyst.tb_gs_b2r_di_total_car_by_market 
where end_date is null 