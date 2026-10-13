-- Date dimension covering every day of the months loaded into RAW. The range comes from
-- the source files, not from pickup timestamps, because a few trips carry bogus dates
-- (2007, for example) that would stretch the calendar by years.

with bounds as (

    select
        min(source_month) as first_day,
        last_day(max(source_month)) as last_day
    from {{ ref('stg_yellow_trips') }}

),

day_offsets as (

    -- seq4() can leave gaps, so number the generated rows instead.
    select row_number() over (order by seq4()) - 1 as day_offset
    from table(generator(rowcount => 3660))

),

days as (

    select dateadd('day', day_offsets.day_offset, bounds.first_day) as date_day
    from bounds
    inner join day_offsets
        on day_offsets.day_offset <= datediff('day', bounds.first_day, bounds.last_day)

)

select
    date_day,
    year(date_day) as year_number,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    date_trunc('month', date_day)::date as month_start_date,
    day(date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week_iso,
    dayname(date_day) as day_name,
    weekiso(date_day) as week_of_year_iso,
    dayofweekiso(date_day) in (6, 7) as is_weekend
from days
