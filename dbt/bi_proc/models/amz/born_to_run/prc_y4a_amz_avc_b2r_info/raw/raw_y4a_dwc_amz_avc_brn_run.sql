{{
  config(
    materialized = 'ephemeral'
    )
}}

select

    accepted_quantity,
    accepted_value,
    "asin",
    country,
    currency,
    offer_id,
    offer_name,
    offer_state,
    po_window_start_date,
    preference,
    product_cost,
    retention_fees_percentage,
    run_date,
    sell_through_end_date,
    sell_through_start_date,
    sold_quantity,
    "status",
    status_description,
    submission_date,
    submitted_quantity,
    vendor_code,
    vendor_responsible_quantity
	
from y4a_cdm.y4a_dwc_amz_avc_brn_run
where submission_date >= current_date - interval '{{ var('born_to_run_days_back') }}' day
{# where submission_date >= current_date - interval '1' year #}
and submission_date <= current_date + interval'1'day