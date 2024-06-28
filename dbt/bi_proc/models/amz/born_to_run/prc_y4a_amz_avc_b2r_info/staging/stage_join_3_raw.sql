{{
  config(
    materialized = 'ephemeral'
  )
}}

select
    a.de_updateddate,
    a.offer_id,
    a.offer_name,
    b.poid,
    a.asin,
    a.offer_state,
    a.status_overall,
    a.status_extra_data,
    a.b2r_type,
    a.country,
    a.submission_date,
    a.po_window_start_date,
    a.sell_through_start_date,
    a.sell_through_end_date,
    a.preference,
    a.retention_fees_percentage,
    a.product_cost,
    a.currency,

    b2r_submitted_qty,
    b2r_accepted_qty,
    b2r_accepted_value,
    btr_vendor_responsible_qty,
    b2r_sold_quantity,

    a.vendor_code as b2r_vendor_code,
    trim(both ' ' from split_part(a.status_extra_data->>'status', ':', 2)) as status_detail_reason,
    a.status_extra_data->>'status_description' as status_description,


    --# thong tin overall cua nhung po co b2r
    b.vendor as po_vendor,

    po_totalqty,
    po_totalcost,
    po_total_case,
    po_submitted_items,
    po_submitted_qty,
    po_accepted_items,
    po_accepted_qty,
    po_received_items,
    po_received_qty,
    po_extra_data,

    -- thong tin asin cua nhung po b2r

    po_asin_qtysubmit,
    po_asin_qtyaccept,
    po_asin_qtyacceptedit,
    po_asin_qtyacceptprev,
    po_asin_qtyreceived,
    po_asin_qtyoutstanding,
    po_asin_unitcost,

    row_number() over(partition by a.offer_id, a.asin order by c.poid asc) as rn_b2r_info,
    row_number() over(partition by a.offer_id, b.poid order by c.asin asc) as rn_b2r_po_info

from {{ ref('stage_avc_brn_run') }} a
left join {{ ref('stage_avc_brn_run_pco') }} b on a.offer_id = b.offerid
left join {{ ref('stage_tb_y4a_amz_avc_diwh_po_dtl') }} c on a.asin = c.asin and b.poid = c.poid
