{{
  config(
    materialized = 'ephemeral'
    )
}}

select * from {{ ref('sop_sales_order_avc_di') }}
union all
select * from {{ ref('sop_sales_order_avc_wh') }}