{{
  config(
    materialized = 'ephemeral'
    )
}}

select
	offer_id,
	"asin",
	country,
	sell_through_start_date,
	sell_through_end_date,
	sell_through_days_left,
	actual_sell_through_period,
	quantity_sold
	
from y4a_cdm.y4a_dwc_amz_avc_brn_run_dtl_itm