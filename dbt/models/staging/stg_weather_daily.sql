-- One row per calendar day. Each raw row is one API response whose daily values are
-- parallel arrays; flatten the date array and pick the same index from the others.

with source as (

    select * from {{ source('raw', 'weather_daily_json') }}

),

flattened as (

    select
        day.value::date as weather_date,
        payload:daily:temperature_2m_max[day.index]::number(5, 1) as temperature_max_c,
        payload:daily:temperature_2m_min[day.index]::number(5, 1) as temperature_min_c,
        payload:daily:temperature_2m_mean[day.index]::number(5, 1) as temperature_mean_c,
        payload:daily:precipitation_sum[day.index]::number(6, 2) as precipitation_mm,
        payload:daily:rain_sum[day.index]::number(6, 2) as rain_mm,
        payload:daily:snowfall_sum[day.index]::number(6, 2) as snowfall_cm,
        payload:daily:wind_speed_10m_max[day.index]::number(5, 1) as wind_speed_max_kmh,
        payload:daily:wind_gusts_10m_max[day.index]::number(5, 1) as wind_gusts_max_kmh,
        payload:latitude::number(8, 5) as grid_latitude,
        payload:longitude::number(8, 5) as grid_longitude,
        _source_file as source_file,
        _loaded_at as loaded_at
    from source,
        lateral flatten(input => payload:daily:time) as day

)

select
    *,
    coalesce(precipitation_mm >= {{ var('rainy_day_precipitation_mm') }}, false) as is_rainy_day,
    coalesce(snowfall_cm > 0, false) as is_snowy_day
from flattened
