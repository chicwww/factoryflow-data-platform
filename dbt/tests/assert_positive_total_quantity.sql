{{
  config(
    severity = 'error',
    tags = ['quality_rule']
  )
}}

select
    production_day_id,
    machine_id,
    total_quantity
from {{ ref('mart_production_daily') }}
where total_quantity <= 0
