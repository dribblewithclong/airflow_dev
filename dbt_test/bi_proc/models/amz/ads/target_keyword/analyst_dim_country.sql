{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
    country_code_3,
    country_code_2
from y4a_analyst.dim_country
group by 1,2