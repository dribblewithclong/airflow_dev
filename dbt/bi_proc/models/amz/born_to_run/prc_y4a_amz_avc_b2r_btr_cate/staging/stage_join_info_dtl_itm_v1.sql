{{
  config(
    materialized = 'ephemeral'
  )
}}

SELECT 
    a.de_updateddate, 
    a.offer_id, 
    a."asin",
    
    CASE 
        WHEN a.offer_state = 'Completed' OR (a.sell_through_end_date::date - CURRENT_DATE::date) < 0 THEN 'Completed'
        WHEN a.sell_through_start_date::date > CURRENT_DATE::date THEN 'Not start ST'
        ELSE 'Active' 
    END AS offer_state,
    
    a.country, 
    a.b2r_type, 
    a.b2r_vendor_code, 
    a.submission_date, 
    a.sell_through_start_date, 
    a.sell_through_end_date,
    a.po_asin_amazon_status, 
    a.po_status, 
    a.cancel_reason, 
    a.is_cancelled_po, 
    a.is_STstartdate_future,
  
    CASE 
        WHEN (a.sell_through_end_date::date - CURRENT_DATE::date) > 0 AND a.sell_through_start_date::date <= CURRENT_DATE::date THEN (a.sell_through_end_date::date - CURRENT_DATE::date + 1) 
        WHEN (a.sell_through_end_date::date - CURRENT_DATE::date) > 0 AND a.sell_through_start_date::date > CURRENT_DATE::date THEN (a.sell_through_end_date::date - a.sell_through_start_date::date)::NUMERIC
        ELSE NULL 
    END::NUMERIC AS days_remaining,
    (a.sell_through_end_date::date - a.sell_through_start_date::date)::NUMERIC AS sell_through_period,
    
    a.product_cost,
    a.preference,
    a.retention_fees_percentage,
    b.sell_through_start_date AS sell_through_start_date_UI,
    b.sell_through_end_date AS sell_through_end_date_UI,
    b.sell_through_days_left AS sell_through_days_left_UI,
    b.actual_sell_through_period,
    b.quantity_sold AS quantity_sold_ui,
    
    SUM(
        CASE 
            WHEN a.offer_state = 'Active' THEN ROUND((a.b2r_sold_quantity / NULLIF(((CURRENT_DATE::date - 1) - a.sell_through_start_date::date + 1), 0) * (a.sell_through_end_date::date - a.sell_through_start_date::date + 1)), 0)
            WHEN a.offer_state = 'Completed' THEN a.b2r_sold_quantity
        END
    ) AS ending_qty,
    SUM(a.btr_vendor_responsible_qty) AS btr_vendor_responsible_qty,
    SUM(a.b2r_sold_quantity) AS b2r_sold_quantity
    

FROM {{ ref('main_tb_y4a_amz_avc_b2r_info') }} a 
LEFT JOIN {{ ref('raw_y4a_dwc_amz_avc_brn_run_dtl_itm') }} b
    ON a.offer_id = b.offer_id 
    AND a."asin" = b."asin"
WHERE 1 = 1
    AND submission_date >= current_date - interval '{{ var('born_to_run_days_back') }}' day
    AND a.submission_date <= current_date + interval'1'day
    AND a.status_overall = 'Accepted'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25