{{
  config(
    materialized = 'ephemeral'
    )
}}

select

  a.*,
  case when NPD_month is null then 'Existing' else concat('NPD ', left(NPD_month,4)) end as NPD_Group

from {{ ref('finance_dim_pims_product_profile') }} a
left join {{ ref('stage_pims_prd_prf_v1') }} b on a.sku = b.sku