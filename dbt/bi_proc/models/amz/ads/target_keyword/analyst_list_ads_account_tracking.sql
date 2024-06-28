{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
    account_id,
    sales_team,
    country,
    country_code
from y4a_analyst.list_ads_account_tracking
where marketplace != 'Perpetua'
group by 1,2,3,4