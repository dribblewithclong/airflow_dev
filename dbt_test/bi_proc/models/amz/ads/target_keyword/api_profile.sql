{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
	account_type,
	account_name,
	country_code,
	currency_code,
	profile_id,
	account_id_format
from y4a_cdm.y4a_dwa_amz_ads_prf