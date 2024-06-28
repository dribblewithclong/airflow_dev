{{
  config(
    materialized = 'ephemeral'
    )
}}

select 

	*, 
	concat(sku, platform, country) as keysaleteam

from y4a_finance.dim_sale_team_by_country_sku_platform