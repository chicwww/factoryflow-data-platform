with source as (
    select * from {{ source('raw', 'maintenance_events') }}
)

select
    maintenance_id,
    machine_id,
    start_time,
    end_time,
    extract(epoch from (end_time - start_time)) / 3600.0 as duration_hours,
    maintenance_type,
    technician_id,
    ingested_at
from source
