
with 
-- Import CTEs
fact_real_stockstats as (
    select * from {{ source('mkt_optimizer','fact_real_stockstats') }}
),
fact_leadsbycar_otherdata as (
    select * from {{ source('mkt_optimizer','fact_leadsbycar_otherdata') }}
),
online_cartracking as (
    select * from {{ source('mkt_optimizer','online_cartracking') }}
),
ctrl_online_cis_buysource as (
    select * from {{ source('mkt_optimizer','ctrl_online_cis_buysource') }}
),
-- Logical CTEs
tmp_stock as 
(
    SELECT aaaid, MAX(frs.`Stock Date`) max_stock FROM {{ source('mkt_optimizer','fact_real_stockstats') }} AS frs
			WHERE  frs.aaaid IN ( 900321655, 900321453, 900305968)
			GROUP BY aaaid
)
select distinct
    -- ids
    frs.aaaid,
    oct.`Car No` as CarNo,
     oct.`Car Licence Number` AS SPZ,
    -- dimensions
    frs.`Country Code` as Country,
    frs.`Fuel Type` as FuelType,
    frs.Make,
    frs.Model,  
    oct.Photographed as Photo,
    oct.`Alternative Fuel Type` AS AlternativeFuelType,
    frs.`Production Year` as ProductionYear,  
    frs.`Model Type` AS Body, 
    CASE 
        WHEN oct.`Gearbox` = 'Manual' THEN 'Manual'
        WHEN oct.`Gearbox` IS NOT NULL THEN 'Automat'
    END AS `GearBox`,
    o.`PriceRange`,
    o.`AgeRange`,
    o.`KM range`,
    oct.Category,
    coalesce(oct.`Drive`, 'Unknown') as `Drive`,
    cbs.`Buy_source`,
    cbs.Sub_Group,
    oct.History,
    oct.`Commisional`					AS Commisional,
    oct.`Buying Currency`				AS Buying_Currency,
    oct.`Not Pass Technical Control`	AS Not_Pass_Tech_Ctrl,
    -- measures
    frs.`Selling Profit` as SellingProfit,
    frs.`Selling Price` AS DummyPrice,
    frs.WebPosition LastWP,
    frs.`Days on Display` AS `DIS`,
    frs.`Tachometer KM` as Tachometer,
    frs.StockTurn,
    -- date/times
    frs.`Stock Date` as ActualDate,
    oct.`Reserved Date From`			AS Reserver_dt_from,
    oct.`Reserved Date To`				AS Reserved_dt_to
    -- metadata
from fact_real_stockstats frs 
left join fact_leadsbycar_otherdata o on o.aaaid = frs.aaaid and o.ActualDate = frs.`Stock Date`
left join online_cartracking oct on oct.aaaid = frs.aaaid
left join ctrl_online_cis_buysource cbs on cbs.id_buy_source = oct.`Buy Source`
left join tmp_stock on tmp_stock.max_stock = frs.`Stock Date` and tmp_stock.aaaid = frs.aaaid
where frs.`Stock Date` = '2022-11-10' and tmp_stock.aaaid is null