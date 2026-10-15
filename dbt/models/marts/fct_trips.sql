{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_file',
        on_schema_change='append_new_columns'
    )
}}

-- Trip fact, one row per raw trip (valid or not; see is_valid_trip).
--
-- Incremental by source file: a run picks up files loaded into RAW since the last build
-- and replaces each one wholesale (delete by source_file, then insert). A file is always
-- loaded in a single COPY, so all of its rows share one loaded_at and it is never split.
-- Replacing whole files, rather than merging on trip_key, means a republished file with
-- fewer rows leaves nothing stale behind.

select
    trip_key,

    pickup_at::date as pickup_date,
    hour(pickup_at) as pickup_hour,
    pickup_zone_id,
    dropoff_zone_id,

    vendor_id,
    rate_code_id,
    rate_code_name,
    payment_type_id,
    payment_type_name,
    store_and_fwd_flag,

    pickup_at,
    dropoff_at,
    trip_duration_minutes::number(12, 2) as trip_duration_minutes,
    passenger_count,
    trip_distance_miles,

    fare_amount,
    extra_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    cbd_congestion_fee_amount,
    total_amount,
    case
        when fare_amount >= {{ var('min_fare_for_tip_pct') }}
            then (tip_amount / fare_amount * 100)::number(12, 2)
    end as tip_pct,

    is_pickup_outside_source_month,
    is_non_positive_duration,
    is_excessive_duration,
    is_negative_amount,
    is_fare_outlier,
    is_distance_outlier,
    is_zero_distance,
    is_unknown_zone,
    is_valid_trip,

    source_month,
    source_file,
    batch_id,
    loaded_at

from {{ ref('stg_yellow_trips') }}

{% if is_incremental() %}
where loaded_at > (select coalesce(max(loaded_at), '1900-01-01'::timestamp_ltz) from {{ this }})
{% endif %}
