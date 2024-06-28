{{
  config(
    materialized = 'ephemeral'
    )
}}

select

    *

from y4a_finance.dim_pims_product_profile