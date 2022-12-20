SELECT
				ct.navision_central_car_tracking_id,
				ct.car_no,
				ct.buy_source,
				ct.current_country,
				ct.current_currency,
				fb.first_branch_cross_country,
				fb.first_country,
				fb.first_branch_in_current_country,
				fb.first_stock_date_in_current_country,
				im1.last_buy_date_cross_country,
				im1.last_buy_date_current_country,
				ec1.buying_date_excompany,
				ic1.buying_date_intercompany,
				CASE
					WHEN ct.buy_source IN ( 0, 1, 3, 6 ) 
					THEN
						COALESCE(ic1.buying_date_intercompany, ct.buying_date)
					WHEN ct.buy_source IN ( 2, 4, 5, 8, 9, 10, 11 ) 
					THEN
						COALESCE(im1.last_buy_date_current_country, im1.last_buy_date_cross_country, ct.buying_date)
					WHEN ct.buy_source = 7 
					THEN 
						COALESCE(ec1.buying_date_excompany, '17530101')
					WHEN ct.buy_source = 12 
					THEN 
						COALESCE(ic1.buying_date_intercompany, ec1.buying_date_excompany)
					ELSE
						ct.buying_date
					END 
					AS buying_date_dwh,
				CASE
					WHEN ct.buy_source IN ( 0, 1, 3, 6 ) 
					THEN
						COALESCE(ic1.buying_date_intercompany, ct.buying_date)
					WHEN ct.buy_source IN ( 2, 4, 5, 8, 9, 10, 11 ) 
					THEN
						COALESCE(im1.last_buy_date_current_country, im1.last_buy_date_cross_country, ct.buying_date)
					WHEN ct.buy_source = 7 
					THEN 
						COALESCE(ec1.buying_date_excompany, '17530101')
					WHEN ct.buy_source = 12 
					THEN 
						COALESCE(ic1.buying_date_intercompany, ec1.buying_date_excompany)
					ELSE
						ct.buying_date
					END 
					AS buying_date_dwh_original
				,ct.is_deleted
				FROM 
					{{ ref('stg_all_cars') }} AS ct
				LEFT JOIN 
					{{ ref('stg_import_country') }} AS im1
					ON ct.car_no = im1.car_no
				LEFT JOIN 
					{{ ref('stg_excompany') }} AS ec1
					ON ct.car_no = ec1.car_no
				LEFT JOIN 
					{{ ref('stg_intercompany') }} AS ic1
					ON ct.car_no = ic1.car_no
				LEFT JOIN 
					{{ ref('stg_first_branch') }} AS fb
					ON ct.car_no = fb.car_no
				WHERE 
					ct.current_country <> ''