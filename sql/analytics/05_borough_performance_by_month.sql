-- How do boroughs compare month to month? Trips, revenue and averages by pickup borough.
select
    month_start_date,
    borough,
    sum(trip_count) as trips,
    round(100 * ratio_to_report(sum(trip_count)) over (partition by month_start_date), 1)
        as pct_of_month_trips,
    round(sum(total_revenue)) as total_revenue,
    round(sum(total_revenue) / sum(trip_count), 2) as revenue_per_trip,
    round(sum(avg_trip_distance_miles * trip_count) / sum(trip_count), 2) as avg_distance_miles
from {marts}.agg_zone_performance
group by month_start_date, borough
order by month_start_date, trips desc
