{{
    config(
        materialized='incremental',
        unique_key='submission_date',
        partition_by={"field": "submission_date", "data_type": "date"}
    )
}}

select 

    a.de_updateddate,
    a.offer_id,
    a.asin,
    a.offer_state,
    a.country,
    a.b2r_type,
    a.b2r_vendor_code,
    a.submission_date,
    a.sell_through_start_date,
    a.sell_through_end_date,
    a.po_asin_amazon_status,
    a.po_status,
    a.cancel_reason,
    a.is_cancelled_po,
    a.is_ststartdate_future,
    a.days_remaining,
    a.sell_through_period,
    a.ending_qty,
    a.btr_vendor_responsible_qty,
    a.b2r_sold_quantity,
    a.product_cost,
    a.preference,
    a.retention_fees_percentage,
    a.sell_through_start_date_ui,
    a.sell_through_end_date_ui,
    a.sell_through_days_left_ui,
    a.actual_sell_through_period,
    a.quantity_sold_ui,
    a.offer_cate_old,
    a.offer_cate,
    a.percent_ending_target,
    a.retention_fees,
    a.days_percent_passed,
    a.pims_sku,
    a.keysaleteam,

    d.eta_date,
    case when e.asin is null then 0 else 1 end as is_currently_in_promo,
    type_active,
    salesmanagername,
    salesleadername,
    salespicname,
    department,
    npd_group,
    mastercategory

from {{ ref('stage_join_info_dtl_itm_v4') }} a

left join {{ ref('finance_dim_sale_team_by_country_sku_platform') }} b on a.keysaleteam = b.keysaleteam

left join {{ ref('stage_pims_prd_prf_v2') }} c on a.pims_sku = c.sku

left join {{ ref('analyst_test') }} d on a.offer_id = d.offer_id

left join {{ ref('analyst_tb_b2r_promo') }} e on a.asin = e.asin and a.country = e.country