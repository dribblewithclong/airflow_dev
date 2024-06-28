{{
  config(
    materialized = 'ephemeral'
    )
}}


select
  report_date, 
  offer_ETA, 
  quantity_sellout, 
  SUM(quantity_sellout) OVER (PARTITION BY offer_ETA ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_ETA
from {{ ref('stage_br2_info_max_logdate_v2') }}