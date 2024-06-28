{{
  config(
    materialized = 'ephemeral'
    )
}}

select

    asin,
    country,
    currency,
    offer_id,
    offer_name,
    offer_state,
    po_window_start_date,
    preference,
    retention_fees_percentage,
    sell_through_end_date,
    sell_through_start_date,
    submission_date,
    vendor_code,

    a.run_date as de_updateddate,
    submitted_quantity as b2r_submitted_qty,
    accepted_quantity as b2r_accepted_qty,

    coalesce(vendor_responsible_quantity,0) as btr_vendor_responsible_qty,
    coalesce(sold_quantity,0) as b2r_sold_quantity,

    case 
        when lower(status) ~ 'not accepted' then 'Not Accepted'
        when lower(status) ~ 'rejected' then 'Rejected'
        else status 
    end as status_overall,

    case
        when vendor_code  in ('YES4A','YES4M')
        then 'AVC WH' 
        else 'AVC DI' 
    end as b2r_type,

    json_strip_nulls(
        json_build_object(
            'status',status,
            'status_description',status_description
            )
        ) as status_extra_data,

    product_cost * to_usd as product_cost,
    accepted_value * to_usd as b2r_accepted_value 

from {{ ref('raw_y4a_dwc_amz_avc_brn_run') }} a
left join {{ ref('analyst_dim_currency_exchange_rate') }} b on a.currency = b.currency_code and a.submission_date = b.run_date
