select
    lake,
    forecast_time,
    count(*) as row_count
from {{ ref('stg_weather_latest') }}
group by lake, forecast_time
having count(*) > 1
