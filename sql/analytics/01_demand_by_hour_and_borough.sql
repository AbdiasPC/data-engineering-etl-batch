-- When and where is demand? Valid trips by pickup hour, split by pickup borough.
select
    trips.pickup_hour,
    count(*) as total_trips,
    count_if(zones.borough = 'Manhattan') as manhattan,
    count_if(zones.borough = 'Queens') as queens,
    count_if(zones.borough = 'Brooklyn') as brooklyn,
    count_if(zones.borough = 'Bronx') as bronx,
    count_if(zones.borough not in ('Manhattan', 'Queens', 'Brooklyn', 'Bronx')) as other,
    round(100 * ratio_to_report(count(*)) over (), 1) as pct_of_all_trips
from {marts}.fct_trips as trips
inner join {marts}.dim_zones as zones
    on trips.pickup_zone_id = zones.zone_id
where trips.is_valid_trip
group by trips.pickup_hour
order by trips.pickup_hour
