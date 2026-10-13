-- One row per raw trip: renamed, cast, decoded and flagged. No rows are dropped here;
-- questionable records are marked so downstream models choose what to exclude.

with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['_source_file', '_file_row_number']) }} as trip_key,

        -- The month the file claims to cover, from yellow_tripdata_YYYY-MM.parquet.
        to_date(regexp_substr(_source_file, '[0-9]{4}-[0-9]{2}') || '-01') as source_month,

        vendorid::integer as vendor_id,
        ratecodeid::integer as rate_code_id,
        payment_type::integer as payment_type_id,
        pulocationid::integer as pickup_zone_id,
        dolocationid::integer as dropoff_zone_id,

        -- TIMESTAMP_NTZ in New York local time, as recorded by the meter.
        tpep_pickup_datetime as pickup_at,
        tpep_dropoff_datetime as dropoff_at,

        passenger_count::integer as passenger_count,
        trip_distance::number(12, 2) as trip_distance_miles,
        upper(trim(store_and_fwd_flag)) as store_and_fwd_flag,

        fare_amount::number(12, 2) as fare_amount,
        extra::number(12, 2) as extra_amount,
        mta_tax::number(12, 2) as mta_tax_amount,
        tip_amount::number(12, 2) as tip_amount,
        tolls_amount::number(12, 2) as tolls_amount,
        improvement_surcharge::number(12, 2) as improvement_surcharge_amount,
        congestion_surcharge::number(12, 2) as congestion_surcharge_amount,
        airport_fee::number(12, 2) as airport_fee_amount,
        cbd_congestion_fee::number(12, 2) as cbd_congestion_fee_amount,
        total_amount::number(12, 2) as total_amount,

        _source_file as source_file,
        _file_row_number as file_row_number,
        _batch_id as batch_id,
        _loaded_at as loaded_at

    from source

),

decoded as (

    select
        *,
        datediff('second', pickup_at, dropoff_at) / 60.0 as trip_duration_minutes,

        -- Code meanings from the TLC yellow taxi data dictionary (2025-03-18).
        case payment_type_id
            when 0 then 'Flex Fare'
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
        end as payment_type_name,

        case rate_code_id
            when 1 then 'Standard rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated fare'
            when 6 then 'Group ride'
            when 99 then 'Unknown'
        end as rate_code_name

    from renamed

),

flagged as (

    select
        *,

        -- Rules that make a trip unusable for analysis.
        coalesce(
            pickup_at < source_month or pickup_at >= dateadd('month', 1, source_month), true
        ) as is_pickup_outside_source_month,
        coalesce(dropoff_at <= pickup_at, true) as is_non_positive_duration,
        coalesce(
            trip_duration_minutes > {{ var('max_trip_duration_minutes') }}, false
        ) as is_excessive_duration,
        coalesce(fare_amount < 0 or total_amount < 0, false) as is_negative_amount,
        coalesce(fare_amount > {{ var('max_fare_amount') }}, false) as is_fare_outlier,
        coalesce(
            trip_distance_miles > {{ var('max_trip_distance_miles') }}, false
        ) as is_distance_outlier,

        -- Informational: worth knowing, but not a reason to exclude the trip.
        coalesce(trip_distance_miles = 0, false) as is_zero_distance,
        coalesce(pickup_zone_id in (264, 265) or dropoff_zone_id in (264, 265), false)
            as is_unknown_zone

    from decoded

)

select
    *,
    not (
        is_pickup_outside_source_month
        or is_non_positive_duration
        or is_excessive_duration
        or is_negative_amount
        or is_fare_outlier
        or is_distance_outlier
    ) as is_valid_trip
from flagged
