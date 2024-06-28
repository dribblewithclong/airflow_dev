{{
  config(
    materialized = 'ephemeral',
    )
}}

select
    date_part('year', min_table_update_date::date) - 1 AS last_year,
    date_trunc('year', min_table_update_date::date)::date - interval'1'year as first_date_last_year,
    min_table_update_date::date - interval'1'month as this_date_last_month,

    date_trunc('month', min_table_update_date::date)::date - interval'12'month AS first_day_last_12_month,
    date_trunc('month', min_table_update_date::date)::date - interval'13'month AS first_day_last_13_month,

    date_trunc('year', min_table_update_date::date)::date - interval'3'year as first_date_last_3_year,
    min_table_update_date::date as min_table_update_date
from y4a_analyst.view_tb_update_date_for_report