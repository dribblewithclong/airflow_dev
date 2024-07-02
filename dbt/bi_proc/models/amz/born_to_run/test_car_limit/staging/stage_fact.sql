{{
    config(
        materialized='ephemeral'
    )
}}

select 
a.run_date::date,
vendor_code,
country,

CASE 
    WHEN vendor_code IN ('YES4A','YES4M') THEN 'WH' 
    ELSE 'DI'
END AS channel,

sum(vendor_responsible_quantity * product_cost*to_usd) as vendor_responsible_value,
-sum(sold_quantity*product_cost*to_usd) sold_value

from {{ ref('stage_b2r_his_v2') }} a

left join {{ ref('analyst_dim_currency_exchange_rate') }} c on a.currency = c.currency_code and a.submission_date = DATE(c.run_date)

where submission_date >= '2024-01-01'
and offer_state = 'Active'
and status = 'Accepted'
and (vendor_responsible_quantity <>0 and vendor_responsible_quantity is not null)
group by 1,2,3,4