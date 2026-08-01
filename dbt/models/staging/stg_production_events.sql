with source as (
    select * from {{ source('raw', 'production_events') }}
)

select
    event_id,
    machine_id,
    event_timestamp,
    date_trunc('day', event_timestamp)::date as production_date,
    product_code,
    quantity_produced,
    unit,
    (unit != 'units') as has_unit_inconsistency,
    shift,
    ingested_at
from source
