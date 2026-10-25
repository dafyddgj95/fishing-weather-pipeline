select distinct on (lake, forecast_time)
    lake,
    forecast_time,
    fetched_at,
    temperature_2m     as temperature_c,
    pressure_msl       as pressure_hpa,
    wind_speed_10m     as wind_speed_mph,
    wind_direction_10m as wind_direction_deg,
    cloud_cover        as cloud_cover_pct,
    precipitation      as precipitation_mm,
    is_day
from {{ source('raw', 'weather_forecast') }}
order by lake, forecast_time, fetched_at desc
