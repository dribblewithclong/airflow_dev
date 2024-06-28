{{
  config(
    materialized = 'ephemeral'
    )
}}


select 

  a.*,
  b.offer_id as offer_ST,
  c.offer_id as offer_ETA

from {{ ref('stage_finance_consolidate') }} a

left join {{ ref('stage_br2_info_max_logdate_v1') }} b on a.platform_prd_id = b.asin and report_date between b.sell_through_start_date and b.sell_through_end_date

left join {{ ref('stage_br2_info_max_logdate_v1') }} as c on a.platform_prd_id = c.asin and report_date between c.salable_date and c.sell_through_end_date