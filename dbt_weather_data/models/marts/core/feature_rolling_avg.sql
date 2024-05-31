{{
  config(
    materialized='view',
    unique_key=['location_id', 'datetime_id'],
    indexes=[
      {'columns': ['location_id', 'datetime_id', 'rolling_id']}
    ],
    on_schema_change='sync_all_columns'
  )
}}

with raw_data as (
  select
  location_id,
  date_trunc('hour', dt.date) as date_hour,
  temp,
  humidity,
  pressure,
  wind_speed
  from {{ ref('fact_weather_actual' )}} as weather
  join {{ ref('dim_datetime') }} as dt
    on weather.datetime_id = dt.datetime_id
),

hourly_agg as (
  select
  location_id,
  date_hour,
  ROUND(CAST(avg(temp) as NUMERIC), 2) as smooth_temp,
  ROUND(CAST(avg(humidity) as NUMERIC), 2) as smooth_humidity,
  ROUND(CAST(avg(pressure) as NUMERIC), 2) as smooth_pressure,
  ROUND(CAST(avg(wind_speed) as NUMERIC), 2) as smooth_wind_speed
  from raw_data
  group by location_id, date_hour
),

rolling_avg as (
  select
  location_id,
  date_hour,
  avg(smooth_temp) over (partition by location_id order by date_hour rows between 11 preceding and current row) as rolling_12hr_avg_temp,
  avg(smooth_humidity) over (partition by location_id order by date_hour rows between 11 preceding and current row) as rolling_12hr_avg_humidity,
  avg(smooth_pressure) over (partition by location_id order by date_hour rows between 11 preceding and current row) as rolling_12hr_avg_pressure,
  avg(smooth_wind_speed) over (partition by location_id order by date_hour rows between 11 preceding and current row) as rolling_12hr_avg_wind_speed
  from hourly_agg
)

select
location_id::text || '_' || dt.datetime_id::text as rolling_id,
location_id,
dt.datetime_id,
ROUND(CAST(rolling_12hr_avg_temp AS NUMERIC) ,2) as rolling_12hr_avg_temp,
ROUND(CAST(rolling_12hr_avg_humidity AS NUMERIC) ,2) as rolling_12hr_avg_humidity,
ROUND(CAST(rolling_12hr_avg_pressure  AS NUMERIC) ,2) as rolling_12hr_avg_pressure,
ROUND(CAST(rolling_12hr_avg_wind_speed AS NUMERIC) ,2) as rolling_12hr_avg_wind_speed
from rolling_avg
join {{ ref('dim_datetime') }} as dt
  on date_hour = dt.date
