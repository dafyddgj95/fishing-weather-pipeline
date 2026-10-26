with curves as (

    select factor, x::numeric as x, y::numeric as y
    from {{ ref('scoring_curves') }}

),

segments as (

    select
        factor,
        x as x0,
        y as y0,
        lead(x) over (partition by factor order by x) as x1,
        lead(y) over (partition by factor order by x) as y1,
        min(x) over (partition by factor) as min_x,
        max(x) over (partition by factor) as max_x
    from curves

),

weather as (

    select * from {{ ref('stg_weather_latest') }}

),

features as (

    select
        w.*,
        extract(month from w.forecast_time)::int as month_num,
        extract(doy from w.forecast_time)::int as day_of_year,
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

scored_parts as (

    select
        f.*,
        {{ curve('water_temp_points', 'f.water_temp_proxy_c') }} as temp_points,
        {{ curve('wind_points', 'f.wind_speed_mph') }} as wind_points,
        {{ curve('pressure_points', 'f.pressure_change_3h_abs_hpa') }} as pressure_points,
        coalesce({{ curve('rain_now_points', 'f.precipitation_mm') }}, 0)
            + coalesce({{ curve('rain_24h_points', 'f.rain_24h_mm') }}, 0) as rain_points,
        {{ curve('season_points', 'f.day_of_year') }} as season_points,
        case
            when coalesce(f.is_day, 1) = 0 then 0
            else (1 - {{ curve('summer_weight', 'f.day_of_year') }})
                    * {{ curve('cloud_points', 'f.cloud_cover_pct') }}
                 + {{ curve('summer_weight', 'f.day_of_year') }}
                    * {{ curve('cloud_points_summer', 'f.cloud_cover_pct') }}
        end as light_points,
        least(
            {{ curve('wind_cap', 'f.wind_speed_mph') }},
            {{ curve('gust_cap', 'f.wind_gust_mph') }},
            {{ curve('rain_now_cap', 'f.precipitation_mm') }},
            {{ curve('water_temp_cap', 'f.water_temp_proxy_c') }}
        ) as score_cap
    from features as f

),

season_checked as (

    select
        p.*,
        coalesce(
            (p.month_num * 100 + extract(day from p.forecast_time)::int)
                between s.season_start_month * 100 + s.season_start_day
                    and s.season_end_month * 100 + s.season_end_day,
            true
        ) as in_season
    from scored_parts as p
    left join {{ ref('lake_seasons') }} as s on s.lake = p.lake

),

totals as (

    select
        *,
        round(
            least(
                coalesce(temp_points, 0) + coalesce(wind_points, 0)
                    + coalesce(pressure_points, 0) + coalesce(rain_points, 0)
                    + coalesce(season_points, 0) + coalesce(light_points, 0),
                coalesce(score_cap, 100)
            )
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
        when fishing_score >= 25 then 'poor'
        else 'very_poor'
    end as rating
from totals
