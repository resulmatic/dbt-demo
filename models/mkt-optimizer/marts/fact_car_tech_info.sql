with total_leads as 
(
    SELECT
			aaaid, SUM(totalLeads) AS TotalLeads, COUNT(DISTINCT actualDate) AS TotalDays		
		FROM {{ ref('stg_current_stock_base_kpi_trends') }}
		GROUP BY aaaid
)
select 
    bc.aaaid,
    bc.CarNo,
    bc.SPZ,
    pc.Movement,
    pc.Reason,
    pc.LastPriceChange,
    History,
    bc.ActualDate, -- pre urcenie posledneho dna
    Country	as Country,
    'https://img.aaaauto.eu/thumb/' + CAST(bc.aaaid AS VARCHAR(20)) + '_card.jpg' AS Image,
    bc.Make,
    bc.Model,
    bc.Make + '-' + bc.Model AS Make_model,
    bc.FuelType,
    bc.AlternativeFuelType,
    bc.Photo,
    bc.ProductionYear,
    bc.SellingProfit,
    bc.Body,
    bc.GearBox,
    bc.DIS,
    bc.PriceRange,
    bc.AgeRange,				
    CASE 
        WHEN bc.Tachometer > 200000 THEN '200k+'
        WHEN bc.Tachometer > 150000 THEN '150 - 200k'
        WHEN bc.Tachometer > 100000 THEN '100 - 150k'
        WHEN bc.Tachometer >  50000 THEN '050 - 100k'
        WHEN bc.Tachometer <=  50000 THEN '000 - 050k'
    END AS KM_Range,
    bc.Drive, 
    bc.tachometer,
    --NULL AS hash_id,
    coalesce(tl.TotalLeads,0) AS TotalLeads,
    tl.TotalDays,
    bc.Buy_source,
    bc.Sub_Group AS Buy_source_Sub_Group,
    category,
    StockTurn,
    LastWP,
    bc.Not_Pass_Tech_Ctrl,
    bc.Reserver_dt_from,
    bc.Reserved_dt_to,
    bc.Commisional AS Commisional,
    bc.buying_currency AS Buying_currency--,
    --hash(bc.Make, bc.Model, bc.FuelType, bc.Body, bc.PriceRange) as hash
from {{ ref('stg_base_cars') }} AS bc
LEFT JOIN total_leads AS tl ON bc.aaaid = tl.aaaid
left JOIN {{ ref('stg_price_changes') }} AS pc ON pc.aaaid = bc.aaaid
