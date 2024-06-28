{{
  config(
    materialized = 'ephemeral'
    )
}}

select
    
    *

from y4a_analyst.test
where rn = 1