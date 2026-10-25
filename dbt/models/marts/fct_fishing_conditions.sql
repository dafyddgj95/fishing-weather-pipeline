with weather as (

    select * from {{ ref('stg_weather_latest') }}

),

features as (

    select
        *,
        pressure_hpa
            - lag(pressure_hpa, 3) over (partition by lake order by forecast_time)
            as pressure_change_3h,
        sum(precipitation_mm)
            over (partition by lake order by forecast_time
                  rows between 23 preceding and current row)
            as rain_24h_mm
    from weather

),

penalties as (

    select
        *,
        least(abs(coalesce(pressure_change_3h, 0)) * 8, 30) as pressure_penalty,
        case
            when wind_speed_mph < 3 then 10
            when wind_speed_mph <= 15 then 0
            else least((wind_speed_mph - 15) * 4, 40)
        end as wind_penalty,
        case when cloud_cover_pct < 20 then 10 else 0 end as cloud_penalty,
        (case when rain_24h_mm > 10 then 15 else 0 end
         + case when precipitation_mm > 1 then 10 else 0 end) as rain_penalty,
        case
            when temperature_c < 4 then 20
            when temperature_c < 8 then 10
            when temperature_c > 20 then 15
            else 0
        end as temperature_penalty
    from features

),

scored as (

    select
        *,
        greatest(
            0,
            100 - pressure_penalty - wind_penalty - cloud_penalty
                - rain_penalty - temperature_penalty
        )::int as fishing_score
    from penalties

)

select
    *,
    case
        when fishing_score >= 75 then 'good'
        when fishing_score >= 50 then 'fair'
        else 'poor'
    end as rating
from scored
