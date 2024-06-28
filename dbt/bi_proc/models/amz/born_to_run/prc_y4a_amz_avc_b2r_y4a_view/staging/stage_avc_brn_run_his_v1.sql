{{
  config(
    materialized = 'ephemeral'
    )
}}

select 
    *,
    ROW_NUMBER() OVER (PARTITION BY run_date::DATE, offer_id, asin ORDER BY sold_quantity DESC) AS rn

from {{ ref('raw_y4a_dwc_amz_avc_brn_run_his') }}
WHERE 
    vendor_code NOT IN ('YES4A', 'YES4M') 
    AND country = 'USA'
    AND status = 'Accepted' 
    AND vendor_responsible_quantity IS NOT NULL
    AND vendor_responsible_quantity <> 0
ORDER BY 
    run_date DESC