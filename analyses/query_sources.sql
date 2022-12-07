select * 
from {{ source('mkt_optimizer','fact_real_stockstats') }}
limit 100