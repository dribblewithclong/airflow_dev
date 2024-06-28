{{
  config(
    materialized = 'ephemeral'
    )
}}

select 

	a.*, 
	b.salable_date

from {{ ref('stage_b2r_info') }} a
left join {{ ref('stage_sop_max_logdate_v2') }} b
using(asin, poid)