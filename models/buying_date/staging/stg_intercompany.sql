SELECT
					wlec.car_no
					,MIN(ne_prodej_date) AS buying_date_intercompany
					FROM
						(
						SELECT
							ROW_NUMBER() OVER (PARTITION BY
												wle.primary_key_field_1_value
												,CAST(wle.date_and_time AS date)
												ORDER BY
												wle.entry_no DESC) 
								AS rn_desc_by_day
							,wle.primary_key_field_1_value AS car_no
							,CAST(wle.date_and_time AS date) AS ne_prodej_date
							,wle.new_value
						FROM
							{{ source('dbt_source_data','raw_hub_hub_navision_front_office_workflow_log_entry') }} AS wle		
						JOIN 
							{{ ref('stg_import_country') }} AS wl
							ON wle.primary_key_field_1_value = wl.car_no
						JOIN 
							{{ ref('stg_all_cars') }} AS ct
							ON ct.car_no = wl.car_no
							AND ct.current_country = wle.country_code -- pouze stavy v aktualni country
						JOIN 
							{{ ref('stg_first_branch') }} AS fb
							ON ct.car_no = fb.car_no
						WHERE
							wle.workflow_template_code = 'NE PRODEJ'
							AND wl.last_buy_date_current_country <= wle.date_and_time -- po poslednim prevozu
							AND NULLIF(NULLIF(ct.branch_id, ''), ct.buy_branch_id) <> fb.first_branch_cross_country -- pouze pokud pobocka/vykupni pobocka nesouhlasi s prvni pobockou
							AND ct.buying_currency <> ct.current_currency -- pouze pokud pobocka/vykupni currency nesouhlasi
							AND ct.purchase_status_code <> 'FINALCHECK'
							AND ct.not_for_sale_status_code <> 'P?EDPRODEJ'
						) AS wlec
					WHERE
						wlec.rn_desc_by_day = 1 -- prvni datum kdy skoncilo v 'JE NA PROD'
						AND wlec.new_value = 'JE NA PROD'
					GROUP BY
						wlec.car_no;