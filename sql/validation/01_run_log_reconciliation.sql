-- Every file currently in RAW must have a latest run-log entry that succeeded (or was
-- skipped as unchanged) and whose source and loaded row counts both equal the rows in
-- RAW right now. {raw} is replaced with the RAW schema name by src/validate.py.

with raw_counts as (

    select _source_file as source_file, count(*) as row_count
    from {raw}.yellow_trips group by _source_file
    union all
    select _source_file, count(*) from {raw}.taxi_zone_lookup group by _source_file
    union all
    select _source_file, count(*) from {raw}.weather_daily_json group by _source_file

),

latest_log as (

    select *
    from {raw}.etl_run_log
    qualify row_number() over (partition by source_file order by started_at desc) = 1

),

compared as (

    select
        raw_counts.source_file,
        latest_log.status in ('SUCCESS', 'SKIPPED')
            and latest_log.source_row_count = raw_counts.row_count
            and latest_log.loaded_row_count = raw_counts.row_count as reconciles
    from raw_counts
    left join latest_log
        on raw_counts.source_file = latest_log.source_file

)

select
    'run_log_reconciles_with_raw' as check_name,
    count(*) > 0 and count_if(not coalesce(reconciles, false)) = 0 as passed,
    count(*) || ' files in RAW, '
        || count_if(not coalesce(reconciles, false)) || ' not reconciled' as details
from compared

union all

select
    'no_unfinished_or_failed_latest_runs',
    count_if(status in ('STARTED', 'FAILED')) = 0,
    count_if(status in ('STARTED', 'FAILED')) || ' files whose latest run is STARTED or FAILED'
from latest_log
