{{
  config(
    materialized='incremental',
    unique_key='dateime_id',
  )
}}

with distinct_times as (
  select distinct
  observation_datetime as datetime
  from {{ ref('stg_weather_actual') }}
)

select
  row_number() over() as datetime_id,
  datetime as date,
  extract('year' from datetime) as year,
  extract('month' from datetime) as month,
  extract('day' from datetime) as day,
  extract('hour' from datetime) as hour,
  0 as minute,
  0 as second,
  extract('dow' from datetime) as day_of_week,
  case when extract('dow' from datetime) in (0, 6) then 1 else 0 end as is_weekend
from distinct_times
{% if is_incremental() %}
where datetime > (select max(datetime) from {{ this }})
{% endif %}