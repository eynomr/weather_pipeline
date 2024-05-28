{{
  config(
    materialized='incremental',
    unique_key='datetime_id',
    indexes=[
      {'columns': ['date']}
    ]
  )
}}

with distinct_times as (
  select distinct event_datetime from (
    select distinct
    date_trunc('minute', observation_datetime) as event_datetime
    from {{ ref('stg_weather_actual') }}
    union all
    select distinct
    date_trunc('minute', forecast_datetime) as event_datetime
    from {{ ref('stg_weather_forecast') }}
  ) a
),

half_hour_intervals as (
  select
  distinct 
  case 
    when extract('minute' from event_datetime) < 30 then date_trunc('hour', event_datetime)
    else date_trunc('hour', event_datetime) + interval '30 minutes'
  end as half_hour
  from distinct_times
)

select
  row_number() over(order by half_hour) + (select coalesce(max(datetime_id), 0) from {{ this }}) as datetime_id,
  half_hour as date,
  extract('year' from half_hour) as year,
  extract('month' from half_hour) as month,
  extract('day' from half_hour) as day,
  extract('hour' from half_hour) as hour,
  extract('minute' from half_hour) as minute,
  0 as second,
  extract('dow' from half_hour) as day_of_week,
  case when extract('dow' from half_hour) in (0, 6) then 1 else 0 end as is_weekend
from half_hour_intervals
{% if is_incremental() %}
where half_hour not in (select date from {{ this }})
{% endif %}