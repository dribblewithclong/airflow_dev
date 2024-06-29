{{
    config(
        materialized='incremental',
        unique_key='vendor_code',
    )
}}

SELECT 
    d.*, 
    vendor_responsible_value, 
    sold_value, 
    car_limit, 
    car_limit - vendor_responsible_value AS available_value,
    (vendor_responsible_value + sold_value) / car_limit AS car_utilization,
    ROW_NUMBER() OVER (PARTITION BY d.vendor_code, d.country ORDER BY d.run_date DESC) AS rn
FROM 
    {{ ref('stage_fact_v3') }} d
LEFT JOIN 
    {{ ref('stage_fact') }} f ON d.run_date = f.run_date
           AND d.country = f.country
           AND d.vendor_code = f.vendor_code
LEFT JOIN 
    {{ ref('stage_total_car') }} c ON d.country = c.country
              AND d.vendor_code = c.vendor_code
ORDER BY 
    d.run_date DESC
