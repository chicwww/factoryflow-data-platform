with maintenance as (
    select * from {{ ref('stg_maintenance_events') }}
),

machines as (
    select * from {{ ref('stg_machines') }}
)

select
    machines.machine_id,
    machines.machine_name,
    machines.machine_type,
    count(maintenance.maintenance_id) as maintenance_event_count,
    coalesce(sum(maintenance.duration_hours), 0) as total_downtime_hours,
    max(maintenance.end_time) as last_maintenance_end
from machines
left join maintenance
    on machines.machine_id = maintenance.machine_id
group by 1, 2, 3
