-- One row per calendar day: valid-trip metrics next to that day's weather. Starts from
-- dim_date so a day with no trips would still appear rather than silently vanish.

with daily_trips as (

    select
        pickup_date,
        count(*) as trip_count,
        sum(total_amount) as total_revenue,
        avg(fare_amount)::number(12, 2) as avg_fare_amount,
        avg(trip_distance_miles)::number(12, 2) as avg_trip_distance_miles,
        avg(trip_duration_minutes)::number(12, 2) as avg_trip_duration_minutes,
        -- Tips are only recorded for card payments, so measure tipping on those alone.
        avg(case when payment_type_id = 1 then tip_pct end)::number(12, 2) as avg_card_tip_pct
    from {{ ref('fct_trips') }}
    where is_valid_trip
    group by pickup_date

)

select
    dim_date.date_day,
    dim_date.day_name,
    dim_date.is_weekend,
    coalesce(daily_trips.trip_count, 0) as trip_count,
    coalesce(daily_trips.total_revenue, 0) as total_revenue,
    daily_trips.avg_fare_amount,
    daily_trips.avg_trip_distance_miles,
    daily_trips.avg_trip_duration_minutes,
    daily_trips.avg_card_tip_pct,
    weather.temperature_mean_c,
    weather.temperature_min_c,
    weather.temperature_max_c,
    weather.precipitation_mm,
    weather.snowfall_cm,
    weather.wind_speed_max_kmh,
    weather.is_rainy_day,
    weather.is_snowy_day
from {{ ref('dim_date') }} as dim_date
left join daily_trips
    on dim_date.date_day = daily_trips.pickup_date
left join {{ ref('stg_weather_daily') }} as weather
    on dim_date.date_day = weather.weather_date
