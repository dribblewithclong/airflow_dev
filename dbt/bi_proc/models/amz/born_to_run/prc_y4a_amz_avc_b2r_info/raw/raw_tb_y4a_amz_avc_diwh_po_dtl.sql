{{
  config(
    materialized = 'ephemeral'
    )
}}

select
    asin,
    country,
    poid,
    qtyaccepted,
    qtyacceptededit,
    qtyacceptedprev,
    qtyoutstanding,
    qtyreceived,
    qtysubmitted,
    unitcost
from y4a_analyst.tb_y4a_amz_avc_diwh_po_dtl
