{{
  config(
    materialized = 'ephemeral'
    )
}}


SELECT  
    TO_CHAR(run_date::DATE, 'YYYY-MM-DD') AS run_date,
    offer_id, 
    asin, 
    offer_state,
    submission_date, 
    vendor_code, 
    sell_through_start_date, 
    sell_through_end_date,
    vendor_responsible_quantity, 
    sold_quantity, 
    product_cost,
    country,
    "status",
    currency
    
FROM 
    y4a_cdm.y4a_dwc_amz_avc_brn_run_his
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14
