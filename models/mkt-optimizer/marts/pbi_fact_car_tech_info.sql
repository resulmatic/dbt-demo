with actual_date as (
        select max(frs.`Stock Date`) as ActualDate from {{ source('mkt_optimizer','fact_real_stockstats') }} AS frs 
		union all 									   
		select max(ActualDate) as ActualDate from {{ source('mkt_optimizer','fact_leadsbycar') }}
		union all									   
		select max(ActualDate) as ActualDate from {{ source('mkt_optimizer','fact_leadsbycar_otherdata') }} 
    ), leasing_prob as 
    (
        SELECT 
			aaaid								AS aaaid, 
			ROUND(leasing_probability_stock, 2) AS  leasing_probability_stock
		FROM {{ ref('stg_current_stock_base_kpi_trends') }}
		WHERE ActualDate = (select min(ActualDate)  from actual_date)   
    )
	 SELECT
		 ci.aaaid												AS aaaid
		,CarNo													AS CarNo
		,SPZ													AS SPZ
		,DIS													AS DIS
		,totalLeads												AS TotalLeads
		--,hash_id												AS hash_id
		,tachometer											AS tachometer
		--,PhotographedDays										AS PhotographedDays
		,Movement												AS Movement
		,LastPriceChange										AS LastPriceChange
		,Reason													AS Reason
		,ROUND(LastWP, 2)										AS WP
		,ROUND(SellingProfit, 2)								AS SellingProfit
		--,ROUND(LastPrice, 2)									AS LastPrice
		,DENSE_RANK() OVER (ORDER BY photo)					AS photo_id
		,DENSE_RANK() OVER (ORDER BY gearBox)					AS gearBox_id
		,DENSE_RANK() OVER (ORDER BY fuelType)				AS fuelType_id
		,DENSE_RANK() OVER (ORDER BY alternativeFuelType)		AS alternativeFuelType_id
		,DENSE_RANK() OVER (ORDER BY drive)					AS drive_id
		,DENSE_RANK() OVER (ORDER BY body)					AS body_id
		,DENSE_RANK() OVER (ORDER BY KM_Range)				AS KM_Range_id
		,DENSE_RANK() OVER (ORDER BY ProductionYear)			AS ProductionYear_id
		,DENSE_RANK() OVER (ORDER BY History)					AS History_id
		,DENSE_RANK() OVER (ORDER BY Category)				AS Category_id
		,DENSE_RANK() OVER (ORDER BY Buy_source)				AS Buy_source_id
		,DENSE_RANK() OVER (ORDER BY Make_model)				AS Make_model_id
		,DENSE_RANK() OVER (ORDER BY Make)					AS Make_id
		,DENSE_RANK() OVER (ORDER BY Model)					AS Model_id
		--,DENSE_RANK() OVER (ORDER BY Branch)					AS Branch_id
		,DENSE_RANK() OVER (ORDER BY PriceRange)				AS PriceRange_id
		,DENSE_RANK() OVER (ORDER BY Country)					AS Country_id
		,ROUND(StockTurn, 2) 									AS StockTurn
		,ci.Not_Pass_Tech_Ctrl									AS Not_Pass_Tech_Ctrl
		,ci.Reserver_dt_from									AS Reserver_dt_from
		,ci.Reserved_dt_to										AS Reserved_dt_to
		,ci.Commisional											AS Commisional
		,ci.buying_currency										AS Buying_currency
		,lp.leasing_probability_stock							AS leasing_probability_stock
		,'https://www.aaaauto.cz/cz/navi.html?aaaid=' + CAST(ci.aaaid AS VARCHAR(20)) AS web
		,'https://img.aaaauto.eu/thumb/' + CAST(ci.aaaid AS VARCHAR(20)) + '_card.jpg' AS Image
	FROM {{ ref('fact_car_tech_info') }} AS ci
	LEFT JOIN leasing_prob AS lp ON lp.aaaid = ci.aaaid 
