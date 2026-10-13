-- Structural: the fact must hold exactly the rows in RAW, file by file. A difference
-- means the incremental model drifted (for example, a file was removed from RAW);
-- rebuild with `dbt build --full-refresh --select fct_trips+`.

with raw_counts as (

    select _source_file as source_file, count(*) as row_count
    from {{ source('raw', 'yellow_trips') }}
    group by _source_file

),

fct_counts as (

    select source_file, count(*) as row_count
    from {{ ref('fct_trips') }}
    group by source_file

)

select
    coalesce(raw_counts.source_file, fct_counts.source_file) as source_file,
    raw_counts.row_count as raw_row_count,
    fct_counts.row_count as fct_row_count
from raw_counts
full outer join fct_counts
    on raw_counts.source_file = fct_counts.source_file
where raw_counts.row_count is distinct from fct_counts.row_count
