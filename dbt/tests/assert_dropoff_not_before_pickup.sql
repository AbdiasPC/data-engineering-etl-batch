-- Known dirty data: a small number of trips have a dropoff time before the pickup time.
-- They are flagged (is_non_positive_duration) and excluded from aggregates; this test
-- keeps the count visible on every run.
{{ config(severity='warn') }}

select trip_key, pickup_at, dropoff_at
from {{ ref('fct_trips') }}
where dropoff_at < pickup_at
