{{
  config(
    materialized = 'ephemeral'
    )
}}

SELECT
    accepted_items,
    accepted_quantity,
    billingaddress_zipcode,
    handoffend,
    handoffstart,
    handofftype,
    legalentityid,
    m2_marketplaceid,
    m2_merchantid,
    offerid,
    paymentterms,
    poid,
    received_items,
    received_quantity,
    shiplocation,
    shippingaddress_addressline1,
    shippingaddress_addressline2,
    shippingaddress_addressname,
    shippingaddress_city,
    shippingaddress_stateorregion,
    "status",
    submitted_items,
    submitted_quantity,
    totalcases,
    totalcost,
    totalquantity,
    vendor,
    vendorstatus
FROM y4a_cdm.y4a_dwc_amz_avc_brn_run_pco