{{
  config(
    materialized = 'ephemeral'
  )
}}

select 
	*, 
	case 
		when preference = 'Return' then 0
		when preference <> 'Return' then (btr_vendor_responsible_qty - ending_qty) * product_cost * retention_fees_percentage
		else 0
	end as retention_fees
from {{ ref('stage_join_info_dtl_itm_v2') }}