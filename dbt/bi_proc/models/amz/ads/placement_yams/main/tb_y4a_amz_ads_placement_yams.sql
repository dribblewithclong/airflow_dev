{{
    config(
        materialized='incremental',
        unique_key='report_date',
        partition_by={"field": "report_date", "data_type": "date"}
    )
}}

with union_all as
(
    select * from {{ ref('stage_spb_placement') }}
    union all
    select * from {{ ref('stage_sps_placement') }}
)

select * from union_all