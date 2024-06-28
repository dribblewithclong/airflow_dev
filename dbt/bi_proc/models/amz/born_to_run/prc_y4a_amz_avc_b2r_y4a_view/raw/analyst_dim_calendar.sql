{{
  config(
    materialized = 'ephemeral'
    )
}}

SELECT 
    dt::date AS date,
    CONCAT("year", '-', "month", '-01')::date AS first_day_of_month,
    "year",
    "month",
    CONCAT(monthnameshort, '-', RIGHT("year"::text, 2)) AS month_year,
    "day",
    dayname,
    weeknum,
    quarter
FROM 
    y4a_finance.dim_calendar dc
WHERE 
    dt BETWEEN '2022-01-01' AND '2025-01-01'
ORDER BY 
    dt DESC
