select 
			 bc.CarNo
			,coalesce(bcd.aaaid , try_cast(bc.CarNo as bigint)) as aaaid
			,bcd.ActualDate 
			,bcd.Tachometer
			,bcd.DummyPrice
			,bcd.WP 							 
			,bcd.AdWithLowerPrice
			,bcd.AdTotalVolume
			,lbc.PriceDrop_DayToDay 			 
			,lbc.PriceDrop_Total 				 
			,lbc.PriceIncrease_DayToDay			 
			,lbc.PriceIncrease_Total 			 
			,lbc.Visits 						 
			,lbc.Visitors 						 
			,lbc.Calls 							 
			,lbc.Forms 							 
			,lbc.Walkins 						 
			,lbc.Appointments 					 
			,lbc.ShowUps 						 
			,lbc.Deals 							 
			,lbc.SoldCall 						 
			,lbc.SoldForm 						 
			,lbc.SoldWalkin  					 
			,o.PriceDrop_Real 
			,o.PriceIncrease_Real  
			,o.NewVisitors 
			,o.webform_liquidity 
			--,o.MarketShare 
			,o.Delay
			,o.WP_Sold 
			,o.Similar_Sold_cars 
			,o.WP_Stock 
			,o.Similar_Stock_cars 
			,o.leasing_probability_stock
from
			 {{ ref('stg_base_cars') }} as bc
		join {{ ref('stg_base_car_days') }} as bcd  on bc.aaaid = bcd.aaaid
		join {{ ref('stg_base_cars_lbc') }} AS lbc  ON lbc.aaaid = bcd.aaaid AND lbc.ActualDate = bcd.ActualDate
		join {{ ref('stg_base_cars_lbc_other') }} AS o  ON o.aaaid = bcd.aaaid AND o.ActualDate = bcd.ActualDate 
