with enriched as (
    select * from {{ ref('int_production_enriched') }}
)

select
    product_code,
    count(check_id) as total_checks,
    count(*) filter (where quality_result = 'pass') as pass_count,
    count(*) filter (where quality_result = 'fail') as fail_count,
    case
        when count(check_id) > 0
            then round(count(*) filter (where quality_result = 'fail')::numeric / count(check_id), 4)
        else null
    end as defect_rate,
    count(*) filter (where defect_type = 'dimension') as defect_dimension_count,
    count(*) filter (where defect_type = 'surface_finish') as defect_surface_finish_count,
    count(*) filter (where defect_type = 'assembly') as defect_assembly_count,
    count(*) filter (where defect_type = 'material') as defect_material_count
from enriched
group by 1
