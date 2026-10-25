select *
from {{ ref('stg_weather_latest') }}
where pressure_hpa not between 900 and 1100
   or wind_speed_mph < 0
   or cloud_cover_pct not between 0 and 100
   or precipitation_mm < 0
