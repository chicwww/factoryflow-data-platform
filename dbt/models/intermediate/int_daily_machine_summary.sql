with enriched as (
    select * from {{ ref('int_production_enriched') }}
)

select
    machine_id || '_' || production_date::text as machine_day_key,
    machine_id,
    machine_type,
    production_date,
    count(*) as event_count,
    sum(quantity_produced) as total_quantity,
    count(check_id) as checked_count,
    count(*) filter (where quality_result = 'fail') as fail_count,
    count(*) filter (where has_unit_inconsistency) as unit_inconsistency_count
from enriched
group by 1, 2, 3, 4
