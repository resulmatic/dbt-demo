with fact_real_stockstats as 
(
    select * from {{ source('mkt_optimizer','fact_real_stockstats') }}
)
select 
        frs.`Stock Date`			as ActualDate, 
        frs.`aaaid`					as aaaid,
        frs.`country Code`			AS Country, 
        frs.`Tachometer Km`			as Tachometer,
        frs.`Selling Price`			as DummyPrice,
        frs.`WebPosition`			as WP, 
        frs.`Ad with lower price`	as AdWithLowerPrice, 
        frs.`Ad Total Volume`		as AdTotalVolume
    from {{ ref('stg_base_cars') }} as b 
    join fact_real_stockstats  frs
        on  frs.aaaid = b.aaaid and b.Country = frs.`Country Code`
