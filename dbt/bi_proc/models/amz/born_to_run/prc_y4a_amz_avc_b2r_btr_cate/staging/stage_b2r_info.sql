{{
  config(
    materialized = 'ephemeral'
    )
}}

select
    *
from {{ ref('main_tb_y4a_amz_avc_b2r_info') }} 
where 1=1
and status_overall = 'Accepted'
and b2r_type = 'AVC DI'
and country = 'USA'
and btr_vendor_responsible_qty <> 0 
and submission_date >= current_date - interval '{{ var('born_to_run_days_back') }}' day