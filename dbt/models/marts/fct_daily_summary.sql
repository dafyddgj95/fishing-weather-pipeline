with daylight as (

    select
        lake,
        forecast_time,
        (forecast_time at time zone 'Europe/London')::date as local_date,
        fishing_score
    from {{ ref('fct_fishing_conditions') }}
    where rating in ('good', 'fair', 'poor')

),

windows as (

    select
        *,
        avg(fishing_score) over (
            partition by lake, local_date
            order by forecast_time
            rows between current row and 2 following
        ) as window_avg_score,
        count(*) over (
            partition by lake, local_date
            order by forecast_time
            rows between current row and 2 following
        ) as window_hours
    from daylight

),

best as (

    select distinct on (lake, local_date)
        lake,
        local_date,
        forecast_time as best_window_start,
        window_avg_score as best_window_score
    from windows
    where window_hours = 3
    order by lake, local_date, window_avg_score desc, forecast_time

),

daily as (

    select
        lake,
        local_date,
        count(*) as daylight_hours,
        round(avg(fishing_score))::int as avg_daylight_score,
        max(fishing_score) as best_hour_score
    from daylight
    group by lake, local_date

)

select
    d.*,
    b.best_window_start,
    round(b.best_window_score)::int as best_window_score,
    case
        when b.best_window_score >= 75 then 'good'
        when b.best_window_score >= 50 then 'fair'
        else 'poor'
    end as best_window_rating
from daily as d
left join best as b using (lake, local_date)
