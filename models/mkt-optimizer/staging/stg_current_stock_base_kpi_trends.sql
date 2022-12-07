SELECT 
            f.CarNo
        ,f.aaaid
        ,f.ActualDate
        ,1.00 * sum(coalesce(PriceDrop_DayToDay, 0))		as PriceDrop_Daily
        ,1.00 * min(coalesce(PriceDrop_Total, 0))			as PriceDrop_Total 
        ,1.00 * sum(coalesce(PriceIncrease_DayToDay, 0))	as PriceIncrease_Daily
        ,1.00 * max(coalesce(PriceIncrease_Total, 0))		as PriceIncrease_Total
        ,1.00 * sum(coalesce(PriceDrop_Real, 0))			as PriceDrop_Real_Daily
        ,1.00 * sum(coalesce(PriceIncrease_Real,0))			as PriceIncrease_Real_Daily
        ,1.00 * sum(coalesce(NewVisitors,0))				as NewVisitors_Daily
        ,1.00 * sum(coalesce(Visits,0))						as Visits_Daily
        ,1.00 * sum(coalesce(Visitors, 0))					as Visitors_Daily
        ,1.00 * sum(coalesce(Calls, 0))						as Calls_Daily
        ,1.00 * sum(coalesce(Forms, 0))						as Forms_Daily
        ,1.00 * sum(coalesce(Walkins, 0))					as Walkins_Daily     
        ,1.00 * sum(coalesce(Appointments, 0))				as Appointments_Daily
        ,1.00 * sum(coalesce(ShowUps, 0))					as ShowUps_Daily
        ,1.00 * sum(coalesce(Deals, 0))						as Deals_Daily
        ,1.00 * sum(coalesce(SoldCall, 0))					as SoldCall_Daily
        ,1.00 * sum(coalesce(SoldForm, 0))					as SoldForm_Daily
        ,1.00 * sum(coalesce(SoldWalkin, 0))				as SoldWalkin_Daily
        ,1.00 * SUM(Calls) + SUM(Forms) + SUM(Walkins)		AS TotalLeads
        ,avg(coalesce(webform_liquidity,0))					as webform_liquidity
        ,avg(coalesce(Delay,0))								as Delay
        ,avg(coalesce(WP_Sold,0))							as WP_Sold
        ,avg(coalesce(Similar_Sold_cars,0))					as Similar_Sold_cars
        ,avg(coalesce(WP_Stock,0))							as WP_Stock
        ,avg(coalesce(Similar_Stock_cars,0))				as Similar_Stock_cars
        ,max(coalesce(leasing_probability_stock,0))			as leasing_probability_stock
        ,AVG(coalesce(CAST(Tachometer AS BIGINT),0)) 		as Tachometer
        ,avg(coalesce(DummyPrice,0))						as DummyPrice
        ,avg(coalesce(WP,0))								as WP
        ,avg(coalesce(AdWithLowerPrice,0))				as Ads_with_lower_price
        ,avg(coalesce(AdTotalVolume,0))					as Ads_Total_Volume
    FROM {{ ref('stg_current_stock_base') }} AS f
    GROUP BY
         f.CarNo
        ,f.aaaid
        ,f.ActualDate