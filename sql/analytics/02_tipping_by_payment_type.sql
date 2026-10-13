-- How does tipping differ by payment type? (TLC records card tips only; cash tips are not captured.)
select
    payment_type_name,
    count(*) as trips,
    round(100 * ratio_to_report(count(*)) over (), 1) as pct_of_trips,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_amount), 2) as avg_tip,
    round(avg(tip_pct), 1) as avg_tip_pct_of_fare,
    round(100 * count_if(tip_amount > 0) / count(*), 1) as pct_trips_with_tip
from {marts}.fct_trips
where is_valid_trip
group by payment_type_name
order by trips desc
