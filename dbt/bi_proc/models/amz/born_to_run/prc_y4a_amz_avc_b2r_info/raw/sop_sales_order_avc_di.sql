{{
  config(
    materialized = 'ephemeral'
    )
}}

select

	"asin",
	'AVC DI' as b2r_type, 
	cancel_reason,
	offer_born_to_run as offer_id,
	po_asin_amazon_status,
	po_name as poid,
	po_status

from y4a_sop_sell_in.sales_order_avc_di
where 1=1
and offer_born_to_run is not null
and qty_request - coalesce(qty_cancel,0) = 0