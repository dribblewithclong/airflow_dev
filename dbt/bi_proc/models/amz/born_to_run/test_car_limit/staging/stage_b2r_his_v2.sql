{{
    config(
        materialized='ephemeral'
    )
}}

select 

    *

from {{ ref('stage_b2r_his') }}
where rn = 1