{{
  config(
    materialized = 'ephemeral'
  )
}}

select 

    asin,
    country,
    type_active

from y4a_analyst.tb_b2r_promo