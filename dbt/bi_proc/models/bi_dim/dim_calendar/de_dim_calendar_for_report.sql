{{
  config(
    materialized = 'table',
    )
}}

select
  a.dt,
  a.year,
  a.year_month_format,
  a."quarter",
  a.quarter_format,
  a."month",
  a.month_format,
  a."day",
  a."dayofweek",
  a."monthname",
  a.monthnameshort,
  a."dayname",
  a.weekstartmonday,
  a.lunardate,
  a.weeknum,
  a.isweekday,

  case when a.year >= b.last_year then 1 else 0 end as isfilterytd,
  case when a.dt >= b.first_date_last_year then 1 else 0 end as isfilterqtd,
  case when a.dt >= b.this_date_last_month then 1 else 0 end as isfiltermtd,
  case when a.dt >= b.first_day_last_12_month then 1 else 0 end as isfilter12months,
  case when a.dt >= b.first_day_last_13_month then 1 else 0 end as isfilter13months,

  b.min_table_update_date as min_of_max_date_report,
  now() as run_time

from y4a_analyst.dim_calendar a
cross join {{ ref('min_date') }} b

where a.dt >= b.first_date_last_3_year
and a.dt <= min_table_update_date 