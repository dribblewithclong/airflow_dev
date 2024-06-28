{{
    config(
        materialized='ephemeral'
    )
}}

select 

    distinct run_date::date as run_date_short,
    *,
    row_number() over(partition by run_date::date, offer_id, asin order by run_date desc) as rn

from {{ ref('raw_y4a_dwc_amz_avc_brn_run_his') }}