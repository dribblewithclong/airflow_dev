{{
  config(
    materialized = 'ephemeral'
    )
}}

select
    platform_product_id,
    pims_sku
from y4a_analyst.dim_pims_platform_product_profile