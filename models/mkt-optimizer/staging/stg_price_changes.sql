WITH cte AS (
			SELECT
				bc.aaaid																AS aaaid
				,coalesce(fpc.Movement, 0)												AS Movement
				,DIS																	AS DIS
				,fpc.DateFrom															AS DateFrom
				,fpc.Reason															AS Reason
				,ROW_NUMBER() OVER (PARTITION BY bc.aaaid ORDER BY fpc.DateFrom desc)	AS RN
			FROM {{ ref('stg_base_cars') }} AS bc
			LEFT JOIN  {{ source('mkt_optimizer','fact_pricechanges') }} AS fpc ON fpc.`Car No` = bc.CarNo AND fpc.Movement <> 0
		)SELECT
			 aaaid
			,max(Movement)	AS Movement
			,max(DateFrom)	AS DateFrom
			,MAX(Reason) AS Reason
			,max(coalesce(DATEDIFF(DAY, DateFrom, GETDATE()), DIS))	AS LastPriceChange
		FROM cte WHERE RN = 1
		GROUP BY cte.aaaid