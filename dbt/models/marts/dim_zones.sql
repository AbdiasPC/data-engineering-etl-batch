-- Taxi zone dimension. Zones 264 and 265 are TLC's placeholders for unknown and
-- out-of-city locations; they are kept so every trip joins, and labelled explicitly.

select
    zone_id,
    coalesce(zone_name, 'Unknown') as zone_name,
    coalesce(borough, 'Outside of NYC') as borough,
    coalesce(service_zone, 'N/A') as service_zone,
    coalesce(service_zone in ('Airports', 'EWR'), false) as is_airport,
    zone_id in (264, 265) as is_unknown_zone
from {{ ref('stg_taxi_zones') }}
