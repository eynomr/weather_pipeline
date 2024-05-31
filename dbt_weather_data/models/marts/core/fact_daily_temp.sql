{{
  config(
    materialized='incremental',
    unique_key=['location_id', 'date'],
    indexes=[
      {'columns': ['location_id', 'date']}
    ],
    on_schema_change='sync_all_columns'
  )
}}

with temp_data as (
  select
  location_id,
  date_trunc('day', dt.date) as date,
  temp,
  temp_min,
  temp_max
  from {{ ref('fact_weather_actual' )}} as weather
  join {{ ref('dim_datetime') }} as dt
    on weather.datetime_id = dt.datetime_id
),

daily_agg as (
  select 
  location_id,
  date,
  ROUND(CAST(avg(temp) as NUMERIC), 2) as avg_temp,
  min(temp_min) as min_temp,
  max(temp_max) as max_temp
  from temp_data
  group by location_id, date
)

select
  location_id,
  concat(location_id, '_', date) as date_location_id,
  date,
  avg_temp,
  min_temp,
  max_temp
from daily_agg
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}