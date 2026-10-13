-- Does rain change daily demand or fares? Rainy (>= 1 mm) vs dry days, weekdays and weekends apart.
select
    case when is_weekend then 'Weekend' else 'Weekday' end as day_type,
    case when is_rainy_day then 'Rainy' else 'Dry' end as weather,
    count(*) as days,
    round(avg(trip_count)) as avg_daily_trips,
    round(avg(avg_fare_amount), 2) as avg_fare,
    round(avg(avg_trip_duration_minutes), 1) as avg_duration_min,
    round(avg(avg_trip_distance_miles), 2) as avg_distance_miles,
    round(avg(precipitation_mm), 1) as avg_precipitation_mm
from {marts}.agg_daily_trips_weather
group by is_weekend, is_rainy_day
order by day_type, weather
