SELECT 
	h.navision_central_car_tracking_id,
    h.`no` AS car_no,
	h.buying_date,
	h.buying_currency,
	h.buy_source,
	h.purchase_status_code,
	h.not_for_sale_status_code,
	h.branch_id,
	h.buy_branch_id,
	h.dwh_created_at,
	h.dwh_updated_at,
	h.is_deleted,
	ic.last_buy_date_cross_country,
	ic.last_buy_date_current_country,
	LEFT(h.system_id, 2) AS current_country,
	CASE WHEN LEFT(h.system_id, 2) = 'CZ' THEN 'CZK'
		WHEN LEFT(h.system_id, 2) = 'PL' THEN 'PLN'
		WHEN LEFT(h.system_id, 2) = 'DE' THEN 'EUR'
		WHEN LEFT(h.system_id, 2) = 'SK' THEN 'EUR'
		WHEN LEFT(h.system_id, 2) = 'HU' THEN 'HUF' END as current_currency
FROM 
    {{ source('dbt_source_data','raw_hub_hub_navision_central_car_tracking') }} AS h
LEFT JOIN {{ ref('stg_import_country') }} AS ic
	ON ic.car_no = h.`no`

	