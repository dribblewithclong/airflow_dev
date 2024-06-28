{{
    config(
        materialized='incremental',
        unique_key='report_date',
        partition_by={"field": "report_date", "data_type": "date"}
    )
}}

select
    *,
    current_timestamp as run_date
{# from y4a_analyst.tb_amz_target_kw_ads #}
from {{ ref('stage_union_all') }}

{% if is_incremental() %}
  where report_date >= (select min(report_date) from {{ ref('stage_union_all') }})
{% endif %}