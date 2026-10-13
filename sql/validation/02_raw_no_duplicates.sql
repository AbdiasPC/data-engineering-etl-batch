-- Idempotency check: re-running a load must never duplicate rows. A row is identified by
-- the file it came from and its position in that file.

select
    'raw_yellow_trips_has_no_duplicates' as check_name,
    count(*) = count(distinct _source_file, _file_row_number) as passed,
    count(*) || ' rows, '
        || count(distinct _source_file, _file_row_number) || ' distinct (file, row number)'
        as details
from {raw}.yellow_trips

union all

select
    'raw_taxi_zones_has_no_duplicates',
    count(*) = count(distinct locationid),
    count(*) || ' rows, ' || count(distinct locationid) || ' distinct zones'
from {raw}.taxi_zone_lookup

union all

select
    'raw_weather_has_one_row_per_file',
    count(*) = count(distinct _source_file),
    count(*) || ' rows, ' || count(distinct _source_file) || ' distinct files'
from {raw}.weather_daily_json
