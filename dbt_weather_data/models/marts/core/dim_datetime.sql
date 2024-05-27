{{
  config(
    materialized='incremental',
    unique_key='datetime_id',
  )
}}

with distinct_times as (
  select distinct event_datetime from (
    select distinct
    observation_datetime as event_datetime
    from {{ ref('stg_weather_actual') }}
    union all
    select distinct
    forecast_datetime as event_datetime
    from {{ ref('stg_weather_forecast') }}
  ) a
)

select
  row_number() over() as datetime_id,
  event_datetime as date,
  extract('year' from event_datetime) as year,
  extract('month' from event_datetime) as month,
  extract('day' from event_datetime) as day,
  extract('hour' from event_datetime) as hour,
  0 as minute,
  0 as second,
  extract('dow' from event_datetime) as day_of_week,
  case when extract('dow' from event_datetime) in (0, 6) then 1 else 0 end as is_weekend
from distinct_times
{% if is_incremental() %}
where event_datetime > (select max(date) from {{ this }})
{% endif %}