{{
  config(
    materialized = 'ephemeral'
    )
}}

select

    country,
    poid,
    asin,

    qtysubmitted as po_asin_qtysubmit,
    qtyaccepted as po_asin_qtyaccept,
    qtyacceptededit as po_asin_qtyacceptedit,
    qtyacceptedprev as po_asin_qtyacceptprev,
    qtyreceived as po_asin_qtyreceived,
    qtyoutstanding as po_asin_qtyoutstanding,
    unitcost as po_asin_unitcost

from {{ ref('raw_tb_y4a_amz_avc_diwh_po_dtl') }}