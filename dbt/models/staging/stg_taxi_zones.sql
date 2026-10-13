-- One row per TLC taxi zone. The source uses the literal string "N/A" for missing values.

with source as (

    select * from {{ source('raw', 'taxi_zone_lookup') }}

)

select
    locationid::integer as zone_id,
    nullif(trim(borough), 'N/A') as borough,
    nullif(trim(zone), 'N/A') as zone_name,
    nullif(trim(service_zone), 'N/A') as service_zone,
    _source_file as source_file,
    _loaded_at as loaded_at
from source
