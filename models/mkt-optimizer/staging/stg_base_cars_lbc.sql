with fact_leadsbycar as 
(
    select * from {{ source('mkt_optimizer','fact_leadsbycar') }}
)
select bcd.aaaid, bcd.ActualDate, 
    avg(lbc.PriceDrop_DayToDay )		as PriceDrop_DayToDay, 
    avg(lbc.PriceDrop_Total )			as PriceDrop_Total, 
    avg(lbc.PriceIncrease_DayToDay )	as PriceIncrease_DayToDay, 
    avg(lbc.PriceIncrease_Total )		as PriceIncrease_Total, 
    avg(lbc.Visits )					as Visits, 
    avg(lbc.Visitors )					as Visitors, 
    avg(lbc.Calls )						as Calls, 
    avg(lbc.Forms )						as Forms, 
    avg(lbc.Walkins )					as Walkins, 
    avg(lbc.Appointments )				as Appointments, 
    avg(lbc.ShowUps )					as ShowUps, 
    avg(lbc.Deals )						as Deals, 
    avg(lbc.SoldCall )					as SoldCall, 
    avg(lbc.SoldForm )					as SoldForm, 
    avg(lbc.SoldWalkin)					as SoldWalkin
from {{ ref('stg_base_car_days') }} as bcd
left join fact_leadsbycar AS lbc ON lbc.aaaid = bcd.aaaid AND lbc.ActualDate = bcd.ActualDate
group by bcd.aaaid, bcd.ActualDate

