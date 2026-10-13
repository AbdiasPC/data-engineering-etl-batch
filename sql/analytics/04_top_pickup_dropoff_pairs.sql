-- Which routes are busiest? Top 10 pickup -> dropoff zone pairs by valid trips.
select
    pickup.zone_name as pickup_zone,
    dropoff.zone_name as dropoff_zone,
    count(*) as trips,
    round(avg(trips.fare_amount), 2) as avg_fare,
    round(avg(trips.trip_distance_miles), 2) as avg_distance_miles,
    round(avg(trips.trip_duration_minutes), 1) as avg_duration_min
from {marts}.fct_trips as trips
inner join {marts}.dim_zones as pickup
    on trips.pickup_zone_id = pickup.zone_id
inner join {marts}.dim_zones as dropoff
    on trips.dropoff_zone_id = dropoff.zone_id
where trips.is_valid_trip
    and not trips.is_unknown_zone
group by pickup.zone_name, dropoff.zone_name
order by trips desc
limit 10
