WITH excomp
	AS 
	(
SELECT
	ct.car_no
	,wle.date_and_time
	,wle.new_value
	,ct.current_country
	,ROW_NUMBER() OVER (PARTITION BY 
								wle.primary_key_field_1_value
								,CAST(wle.date_and_time AS date)
						ORDER BY  
								CASE WHEN -- pokud se li?? country, bereme pouze zmeny na nove country
									ct.current_country <> tnc.first_country-- tj. je jina aktualni branch vs first branch
									AND ct.buying_currency <> ct.current_currency-- tj. je jina aktualni currency vs buying currency			
									AND CAST(wle.date_and_time AS date) >= tnc.first_stock_date_in_current_country -- bereme pouze zmeny po p?evozu
									THEN 1 ELSE 0 END DESC,
										wle.entry_no DESC
									)
										AS rn_desc_by_day
FROM
	{{ ref('stg_all_cars') }} AS ct 	
JOIN 
	 {{ source('dbt_source_data','raw_hub_hub_navision_front_office_workflow_log_entry') }} AS wle
	ON wle.primary_key_field_1_value = ct.car_no	
JOIN 
	{{ ref('stg_first_branch') }} AS tnc
ON wle.primary_key_field_1_value = tnc.car_no
WHERE
	wle.workflow_template_code = 'NE PRODEJ' 
	AND wle.old_value <> ''
	AND ((wle.old_value = 'COMP_CAR' AND wle.new_value = 'JE NA PROD') 
			OR wle.new_value = 'COMP_CAR' OR wle.new_value = 'JE NA PROD')
			)
SELECT 
	wlec.car_no
	,MIN(CAST(wlec.date_and_time AS DATE)) AS buying_date_excompany
FROM  
	excomp AS wlec
WHERE 
	rn_desc_by_day = 1
	AND wlec.new_value = 'JE NA PROD' 
GROUP BY 
	wlec.car_no