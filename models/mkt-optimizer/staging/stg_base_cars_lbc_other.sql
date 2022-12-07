with fact_leadsbycar_otherdata as 
(
    select * from {{ source('mkt_optimizer','fact_leadsbycar_otherdata') }}
)
select bcd.aaaid, bcd.ActualDate, 
			avg(o.PriceDrop_Real )				as PriceDrop_Real, 
			avg(o.PriceIncrease_Real  )			as PriceIncrease_Real,  
			avg(o.NewVisitors )					as NewVisitors, 
			avg(o.webform_liquidity )			as webform_liquidity, 
			avg(o.MedianDelay)					as Delay, 
			avg(o.WP_Sold )						as WP_Sold, 
			avg(o.Similar_Sold_cars )			as Similar_Sold_cars, 
			avg(o.WP_Stock )					as WP_Stock, 
			avg(o.Similar_Stock_cars )			as Similar_Stock_cars, 
			avg(o.leasing_probability_stock )	as leasing_probability_stock
from {{ ref('stg_base_car_days') }} as bcd
left join fact_leadsbycar_otherdata AS o  ON o.aaaid = bcd.aaaid AND o.ActualDate = bcd.ActualDate and bcd.country = o.CountryCode_cc
group by bcd.aaaid, bcd.ActualDate