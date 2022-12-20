SELECT 
	wle.primary_key_field_1_value AS car_no
	,CAST(MAX(wle.date_and_time) AS date) AS last_buy_date_cross_country
	,CAST(MAX(CASE WHEN wle.country_code = LEFT(h.system_id, 2) THEN wle.date_and_time end) AS date) AS last_buy_date_current_country
FROM 
	{{ source('dbt_source_data','raw_hub_hub_navision_front_office_workflow_log_entry') }} AS wle 
JOIN {{ source('dbt_source_data','raw_hub_hub_navision_central_car_tracking') }} AS h
	ON h.no = wle.primary_key_field_1_value
WHERE 
	wle.workflow_template_code = 'VYKUP'
	AND wle.new_value = 'VYKOUPENO' 
GROUP BY 
	wle.primary_key_field_1_value