{{
    config(
        materialized='incremental',
        unique_key='submission_date',
        partition_by={"field": "submission_date", "data_type": "date"}
    )
}}

SELECT
  de_updateddate,
  offer_id,
  offer_name,
  poid,
  asin,
  offer_state,
  status_overall,
  status_extra_data,

  case 
    when status_detail_reason like '%Active%' then 'Active offer'
    when status_detail_reason like '%Hazmat%' then 'Hazardous materials'
    when status_detail_reason like '%Unfav%' then 'Unfavourable'
    when status_description like '%network%' then 'Network temporary constraints'
    when status_detail_reason = 'Retry later' or status_detail_reason is null or (status_detail_reason = '' and status_overall <> 'Accepted') then 'Others'
    {# when status_detail_reason = '' and status_overall <> 'Accepted' #}
    else status_detail_reason 
  end as status_detail_reason,

  status_description,
  b2r_vendor_code,
  a.b2r_type,
  country,
  submission_date,
  po_window_start_date,
  sell_through_start_date,
  sell_through_end_date,
  preference,
  retention_fees_percentage,
  product_cost,
  currency,

  json_strip_nulls(json_build_object(
  'b2r_submitted_qty', b2r_submitted_qty,
  'b2r_accepted_qty', b2r_accepted_qty,
  'b2r_accepted_value', b2r_accepted_value,
  'btr_vendor_responsible_qty', btr_vendor_responsible_qty,
  'b2r_sold_quantity', b2r_sold_quantity)) AS btr_qty_raw,

  {# Chi lay dong record dau tien de sum so cho dung #}
  CASE WHEN rn_b2r_info = 1 THEN b2r_submitted_qty ELSE 0 END AS b2r_submitted_qty,
  CASE WHEN rn_b2r_info = 1 THEN b2r_accepted_qty ELSE 0 END AS b2r_accepted_qty,
  CASE WHEN rn_b2r_info = 1 THEN b2r_accepted_value ELSE 0 END AS b2r_accepted_value,
  CASE WHEN rn_b2r_info = 1 THEN btr_vendor_responsible_qty ELSE 0 END AS btr_vendor_responsible_qty,
  CASE WHEN rn_b2r_info = 1 THEN b2r_sold_quantity ELSE 0 END AS b2r_sold_quantity,

  {# thong tin overall cua nhung PO co B2R: Chi dung cho muc dich referrence de check data #}
  po_vendor,
  CASE WHEN rn_b2r_po_info = 1 THEN po_totalqty ELSE 0 END po_totalqty,
  CASE WHEN rn_b2r_po_info = 1 THEN po_totalcost ELSE 0 END po_totalcost,
  CASE WHEN rn_b2r_po_info = 1 THEN po_total_case ELSE 0 END po_total_case,
  CASE WHEN rn_b2r_po_info = 1 THEN po_submitted_items ELSE 0 END  po_submitted_items,
  CASE WHEN rn_b2r_po_info = 1 THEN po_submitted_qty ELSE 0 END po_submitted_qty,
  CASE WHEN rn_b2r_po_info = 1 THEN po_accepted_items ELSE 0 END po_accepted_items,
  CASE WHEN rn_b2r_po_info = 1 THEN po_accepted_qty ELSE 0 END po_accepted_qty,
  CASE WHEN rn_b2r_po_info = 1 THEN po_received_items ELSE 0 END po_received_items,
  CASE WHEN rn_b2r_po_info = 1 THEN po_received_qty ELSE 0 END po_received_qty,

  po_extra_data,
  {# Thong tin ASIN cua nhung PO B2R #}

  po_asin_qtysubmit,
  po_asin_qtyaccept,
  po_asin_qtyacceptedit,
  po_asin_qtyacceptprev,
  po_asin_qtyreceived,
  po_asin_qtyoutstanding,
  po_asin_unitcost,

  {# ADD THONG TIN CANCEL OFFER VI TEST #}
    po_asin_amazon_status, po_status, cancel_reason,
  case when b.offer_id is null then 0 else 1 end as is_cancelled_po,
  case when sell_through_start_date <= current_date then 0 else 1 end as is_STstartdate_future,

  now() AS bi_runtime

from {{ ref('stage_join_3_raw') }} a
left join {{ ref('stage_union_sop') }} b using(poid, asin, offer_id)