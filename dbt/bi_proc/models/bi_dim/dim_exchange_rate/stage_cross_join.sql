{{
  config(
    materialized = 'ephemeral'
    )
}}

with not_null_join as (select * from {{ ref('stage_join_old_new') }} where currency_code is not null)

select 
    d.dt::date AS run_date,
    t.currency_code
from y4a_analyst.dim_calendar d
cross join not_null_join t
WHERE dt>='2015-01-01' AND dt <= CURRENT_DATE + INTERVAL '3' DAY 