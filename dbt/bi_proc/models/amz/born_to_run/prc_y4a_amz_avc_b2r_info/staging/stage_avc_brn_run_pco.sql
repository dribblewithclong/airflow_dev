{{
  config(
    materialized = 'ephemeral'
    )
}}

select

    offerid,
    poid,
    vendor,

    accepted_items as po_accepted_items,
    accepted_quantity as po_accepted_qty,
    received_items as po_received_items,
    received_quantity as po_received_qty,
    submitted_items as po_submitted_items,
    submitted_quantity as po_submitted_qty,
    totalcases as po_total_case,
    totalcost as po_totalcost,
    totalquantity as po_totalqty,

    json_strip_nulls(
        json_build_object(
            'legalentityid',legalentityid,
            'shiplocation',shiplocation,
            'handofftype',handofftype,
            'handoffstart',handoffstart,
            'handoffend',handoffend,
            'po_status',status,
            'po_vendor_status',vendorstatus,
            'po_paymentterms',paymentterms,
            'shippingaddress_name',shippingaddress_addressname,
            'shippingaddress_line1',shippingaddress_addressline1,
            'shippingaddress_line2',shippingaddress_addressline2,
            'shippingaddress_city',shippingaddress_city,
            'shippingaddress_stateorregion',shippingaddress_stateorregion,
            'billingaddress_zipcode',billingaddress_zipcode,
            'm2_marketplaceid',m2_marketplaceid,
            'm2_merchantid',m2_merchantid
            )
        ) as po_extra_data
        
from {{ ref('raw_y4a_dwc_amz_avc_brn_run_pco') }}