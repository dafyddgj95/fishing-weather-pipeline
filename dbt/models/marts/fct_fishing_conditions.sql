with bands as (

    select * from {{ ref('scoring_bands') }}

),

weather as (

    select * from {{ ref('stg_weather_latest') }}

),

features as (

    select
        w.*,
        extract(month from w.forecast_time)::int as month_num,
        avg(w.temperature_c) over (
            partition by w.lake order by w.forecast_time
            rows between 71 preceding and current row
        ) as water_temp_proxy_c,
        abs(coalesce(
            w.pressure_hpa - lag(w.pressure_hpa, 3) over (
                partition by w.lake order by w.forecast_time
            ), 0
        )) as pressure_change_3h_abs_hpa,
        sum(w.precipitation_mm) over (
            partition by w.lake order by w.forecast_time
            rows between 23 preceding and current row
        ) as rain_24h_mm
    from weather as w

),

banded as (

    select
        f.*,
        {{ band_points('water_temp_proxy_c', 'f.water_temp_proxy_c') }} as temp_points,
        {{ band_points('wind_speed_mph', 'f.wind_speed_mph') }} as wind_points,
        {{ band_points('pressure_change_3h_abs_hpa', 'f.pressure_change_3h_abs_hpa') }} as pressure_points,
        {{ band_points('rain_24h_mm', 'f.rain_24h_mm') }} as rain_points,
        {{ band_points('month', 'f.month_num') }} as season_points,
        case
            when coalesce(f.is_day, 1) = 0 then 0
            when f.month_num in (6, 7, 8)
                then {{ band_points('cloud_summer', 'f.cloud_cover_pct') }}
            else {{ band_points('cloud', 'f.cloud_cover_pct') }}
        end as light_points,
        least(
            {{ band_points('wind_cap', 'f.wind_speed_mph') }},
            {{ band_points('rain_now_cap', 'f.precipitation_mm') }},
            {{ band_points('water_temp_cap', 'f.water_temp_proxy_c') }}
        ) as score_cap
    from features as f

),

season_checked as (

    select
        b.*,
        coalesce(
            (b.month_num * 100 + extract(day from b.forecast_time)::int)
                between s.season_start_month * 100 + s.season_start_day
                    and s.season_end_month * 100 + s.season_end_day,
            true
        ) as in_season
    from banded as b
    left join {{ ref('lake_seasons') }} as s on s.lake = b.lake

),

totals as (

    select
        *,
        least(
            coalesce(temp_points, 0) + coalesce(wind_points, 0)
                + coalesce(pressure_points, 0) + coalesce(rain_points, 0)
                + coalesce(season_points, 0) + coalesce(light_points, 0),
            coalesce(score_cap, 100)
        )::int as fishing_score
    from season_checked

)

select
    *,
    case
        when not in_season then 'closed'
        when coalesce(is_day, 1) = 0 then 'dark'
        when fishing_score >= 75 then 'good'
        when fishing_score >= 50 then 'fair'
        else 'poor'
    end as rating
from totals
