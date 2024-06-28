{{
  config(
    materialized = 'table'
    )
}}

SELECT 
    t1.run_date,
    t1.currency_code,
    COALESCE(t2.to_usd,a.to_usd) AS to_usd,
    COALESCE(t2.to_vnd,a.to_vnd) AS to_vnd,
    NOW() AS bi_run_time
FROM {{ ref('stage_cross_join') }} t1
LEFT JOIN {{ ref('stage_join_old_new') }} t2 ON t1.run_date = t2.run_date AND t1.currency_code = t2.currency_code
LEFT JOIN {{ ref('stage_join_old_new') }} a ON a.currency_code = t1.currency_code AND a.run_date < t1.run_date AND t2.to_usd IS NULL AND a.to_usd IS NOT NULL 
LEFT JOIN {{ ref('stage_join_old_new') }} a1 ON a1.currency_code = t1.currency_code AND a1.run_date < t1.run_date AND t2.to_usd IS NULL AND a1.run_date > a.run_date  AND a1.to_usd IS NOT NULL 
WHERE a1.run_date IS NULL 
ORDER BY 1 desc