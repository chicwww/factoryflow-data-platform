with source as (
    select * from {{ source('raw', 'quality_checks') }}
)

select
    check_id,
    production_event_id,
    check_timestamp,
    result,
    defect_type,
    inspector_id,
    (inspector_id is null) as is_missing_inspector,
    ingested_at
from source
