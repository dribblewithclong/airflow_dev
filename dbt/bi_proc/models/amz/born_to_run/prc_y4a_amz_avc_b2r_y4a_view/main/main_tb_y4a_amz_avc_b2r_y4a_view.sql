{{
    config(
        materialized='incremental',
        unique_key='offer_id',
        partition_by={"field": "submission_date", "data_type": "date"}
    )
}}

SELECT 
    run_date, 
    offer_id, 
    asin, 
    offer_state, 
    vendor_code, 
    submission_date, 
    sell_through_start_date, 
    sell_through_end_date,
    vendor_responsible_quantity,
    sold_quantity AS amz_qty,
    product_cost,
    ((run_date::date - sell_through_start_date) / NULLIF(sell_through_end_date - sell_through_start_date, 0))::DECIMAL(10, 4) AS percent_time_passed,
    LEAST(vendor_responsible_quantity, GREATEST(COALESCE(running_total_st_edited, 0), COALESCE(running_total_eta_edited, 0))) / vendor_responsible_quantity AS percent_units_sold_y4a,
    sold_quantity / vendor_responsible_quantity AS percent_units_sold_amz,
    running_total_st_edited, 
    running_total_eta_edited,
    LEAST(vendor_responsible_quantity, GREATEST(COALESCE(running_total_st_edited, 0), COALESCE(running_total_eta_edited, 0))) AS y4a_qty,
    running_total_st_edited AS st_qty, 
    running_total_eta_edited AS eta_qty,
    ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY run_date DESC) AS rn
FROM 
    {{ ref('stage_b2r_his_st_eta_max_v2') }}
WHERE 
    run_date::date >= CURRENT_DATE - 14
ORDER BY 
    run_date DESC
