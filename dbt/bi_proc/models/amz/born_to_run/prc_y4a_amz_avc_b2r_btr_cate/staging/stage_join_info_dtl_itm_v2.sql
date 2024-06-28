{{
  config(
    materialized = 'ephemeral'
  )
}}

select 
	*,
	case 
		when ending_qty > btr_vendor_responsible_qty then 'Exceed target'
		when ending_qty = btr_vendor_responsible_qty and btr_vendor_responsible_qty > 0 then 'Hit target'
		when btr_vendor_responsible_qty = 0 then 'Not submit PO'
		else 'Miss target'
	end as offer_cate_old,
	
	case 
		when ending_qty >= btr_vendor_responsible_qty then'Hit target'
		else 'Miss target'
	end as offer_cate,
	
	ending_qty / nullif(btr_vendor_responsible_qty, 0) as percent_ending_target

from {{ ref('stage_join_info_dtl_itm_v1') }} 