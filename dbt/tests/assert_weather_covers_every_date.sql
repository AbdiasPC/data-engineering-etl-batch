-- Structural: every day in the loaded months must have a weather row, otherwise the
-- daily mart would report trips with no weather and skew the rain analysis.

select dim_date.date_day
from {{ ref('dim_date') }} as dim_date
left join {{ ref('stg_weather_daily') }} as weather
    on dim_date.date_day = weather.weather_date
where weather.weather_date is null
