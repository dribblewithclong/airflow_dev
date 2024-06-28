{{
  config(
    materialized = 'ephemeral'
    )
}}


select 
    report_date, 
    offer_ST, 
    quantity_sellout, 
    SUM(quantity_sellout) OVER (PARTITION BY offer_st ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_ST
from {{ ref('stage_br2_info_max_logdate_v2') }}