with source as (
    select * from {{ source('raw', 'machines') }}
)

select
    machine_id,
    machine_name,
    machine_type,
    install_date,
    ingested_at
from source
