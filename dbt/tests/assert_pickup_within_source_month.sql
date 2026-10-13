-- Known dirty data: each monthly file contains a few trips whose pickup time falls
-- outside that month (mostly minutes either side of the boundary, occasionally years
-- away). They are flagged and excluded from aggregates.
{{ config(severity='warn') }}

select trip_key, source_file, pickup_at
from {{ ref('fct_trips') }}
where is_pickup_outside_source_month
