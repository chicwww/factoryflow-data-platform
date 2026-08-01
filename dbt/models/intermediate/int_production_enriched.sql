with production as (
    select * from {{ ref('stg_production_events') }}
),

machines as (
    select * from {{ ref('stg_machines') }}
),

quality as (
    select * from {{ ref('stg_quality_checks') }}
),

quality_ranked as (
    select
        *,
        row_number() over (
            partition by production_event_id
            order by check_timestamp desc, check_id desc
        ) as check_rank,
        count(*) over (partition by production_event_id) as check_count_for_event
    from quality
),

quality_deduped as (
    select * from quality_ranked where check_rank = 1
)

select
    production.event_id,
    production.machine_id,
    machines.machine_type,
    production.production_date,
    production.product_code,
    production.quantity_produced,
    production.unit,
    production.has_unit_inconsistency,
    production.shift,
    quality_deduped.check_id,
    quality_deduped.result as quality_result,
    quality_deduped.defect_type,
    coalesce(quality_deduped.check_count_for_event, 0) > 1 as has_duplicate_quality_checks
from production
left join machines
    on production.machine_id = machines.machine_id
left join quality_deduped
    on quality_deduped.production_event_id = production.event_id
