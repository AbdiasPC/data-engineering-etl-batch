-- One row per pickup zone per month: volume, revenue and tipping for valid trips.

with monthly as (

    select
        pickup_zone_id,
        date_trunc('month', pickup_date)::date as month_start_date,
        count(*) as trip_count,
        sum(total_amount) as total_revenue,
        avg(fare_amount)::number(12, 2) as avg_fare_amount,
        avg(trip_distance_miles)::number(12, 2) as avg_trip_distance_miles,
        avg(trip_duration_minutes)::number(12, 2) as avg_trip_duration_minutes,
        avg(case when payment_type_id = 1 then tip_pct end)::number(12, 2) as avg_card_tip_pct,
        (count_if(payment_type_id = 1) / count(*) * 100)::number(5, 2) as card_payment_pct
    from {{ ref('fct_trips') }}
    where is_valid_trip
    group by pickup_zone_id, month_start_date

)

select
    {{ dbt_utils.generate_surrogate_key(['monthly.pickup_zone_id', 'monthly.month_start_date']) }}
        as zone_month_key,
    monthly.pickup_zone_id,
    zones.zone_name,
    zones.borough,
    zones.service_zone,
    monthly.month_start_date,
    monthly.trip_count,
    monthly.total_revenue,
    monthly.avg_fare_amount,
    monthly.avg_trip_distance_miles,
    monthly.avg_trip_duration_minutes,
    monthly.avg_card_tip_pct,
    monthly.card_payment_pct,
    rank() over (
        partition by monthly.month_start_date order by monthly.trip_count desc
    ) as trip_count_rank_in_month
from monthly
inner join {{ ref('dim_zones') }} as zones
    on monthly.pickup_zone_id = zones.zone_id
