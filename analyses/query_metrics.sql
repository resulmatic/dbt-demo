select * 
from {{ metrics.calculate(
    metric('profit_avg_dropped_cars'),
    grain='month',
    dimensions=['Country', 'Make']
) }}
