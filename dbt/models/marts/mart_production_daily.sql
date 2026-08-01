with daily as (
    select * from {{ ref('int_daily_machine_summary') }}
)

select
    machine_day_key as production_day_id,
    machine_id,
    machine_type,
    production_date,
    event_count,
    total_quantity,
    checked_count,
    fail_count,
    case
        when checked_count > 0 then round(fail_count::numeric / checked_count, 4)
        else null
    end as defect_rate,
    unit_inconsistency_count
from daily
