{{
  config(
    severity = 'error',
    tags = ['quality_rule']
  )
}}

select
    production_day_id,
    machine_id,
    defect_rate
from {{ ref('mart_production_daily') }}
where defect_rate is not null
  and (defect_rate < 0 or defect_rate > 1)
