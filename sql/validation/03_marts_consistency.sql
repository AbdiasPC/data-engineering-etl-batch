-- End-to-end consistency between layers: nothing lost from RAW to the fact, and the
-- aggregated marts add up to the valid trips in the fact.

with totals as (

    select
        (select count(*) from {raw}.yellow_trips) as raw_trips,
        (select count(*) from {marts}.fct_trips) as fct_trips,
        (select count_if(is_valid_trip) from {marts}.fct_trips) as valid_trips,
        (select sum(trip_count) from {marts}.agg_daily_trips_weather) as daily_trips,
        (select sum(trip_count) from {marts}.agg_zone_performance) as zone_trips,
        (select count(*) from {marts}.agg_daily_trips_weather) as daily_rows,
        (select count_if(precipitation_mm is null) from {marts}.agg_daily_trips_weather)
            as days_without_weather,
        (select count(*) from {marts}.dim_date) as calendar_days

)

select
    'fct_trips_row_count_equals_raw' as check_name,
    raw_trips > 0 and raw_trips = fct_trips as passed,
    'raw=' || raw_trips || ', fct=' || fct_trips as details
from totals

union all

select
    'daily_mart_sums_to_valid_trips',
    daily_trips = valid_trips,
    'daily mart=' || daily_trips || ', valid trips in fct=' || valid_trips
from totals

union all

select
    'zone_mart_sums_to_valid_trips',
    zone_trips = valid_trips,
    'zone mart=' || zone_trips || ', valid trips in fct=' || valid_trips
from totals

union all

select
    'daily_mart_has_every_day_with_weather',
    daily_rows = calendar_days and days_without_weather = 0,
    daily_rows || ' rows for ' || calendar_days || ' calendar days, '
        || days_without_weather || ' without weather'
from totals
