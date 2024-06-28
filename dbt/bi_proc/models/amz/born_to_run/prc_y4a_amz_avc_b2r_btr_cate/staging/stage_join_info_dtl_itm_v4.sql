{{
  config(
    materialized = 'ephemeral'
  )
}}

select

  a.*,
  (1- (days_remaining / sell_through_period))::numeric as days_percent_passed,
  b.pims_sku,
  concat(pims_sku, 'AMZ', country) keysaleteam

from {{ ref('stage_join_info_dtl_itm_v3') }} a
join {{ ref('analyst_dim_pims_platform_product_profile') }} b
on a."asin" = b.platform_product_id