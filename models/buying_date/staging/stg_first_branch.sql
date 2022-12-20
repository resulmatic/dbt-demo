SELECT
	car_no
	,MAX(CASE WHEN rn = 1 THEN branch_code END) AS first_branch_cross_country
	,MAX(CASE WHEN rn = 1 THEN country_code END) AS first_country
	,MAX(CASE WHEN rn_current_country = 1 THEN branch_code END ) AS first_branch_in_current_country
	,MAX(CASE WHEN rn_current_country = 1 THEN posting_date END) AS first_stock_date_in_current_country
					FROM
						(SELECT
							csle.car_no
							,csle.country_code
							,csle.branch_code
							,csle.posting_date
							,ROW_NUMBER() OVER (PARTITION BY csle.car_no ORDER BY csle.posting_date) AS rn
							,CASE 
								WHEN ct.current_country = csle.country_code
								THEN ROW_NUMBER() OVER (PARTITION BY csle.car_no
															ORDER BY
															CASE WHEN ct.current_country = csle.country_code THEN 1 ELSE 0 END DESC
															,csle.posting_date
														)
								ELSE NULL 
								END 
								AS rn_current_country
						FROM
							{{ source('dbt_source_data','raw_hub_hub_navision_front_office_car_stock_ledger_entry') }} AS csle
						JOIN 
							{{ ref('stg_all_cars') }} AS ct
							ON ct.car_no = csle.car_no
						) AS csle2
					GROUP BY 
						car_no;

						