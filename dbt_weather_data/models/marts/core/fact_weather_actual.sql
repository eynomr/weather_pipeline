{{
  config(
    materialized='incremental',
    unique_key='weather_id',
  )
}}

with weather_stage as (
  select * 
  from {{ ref('stg_weather_actual') }}
)
select
  weather_id,
  location_id,
  dim_datetime.datetime_id,
  dim_weather_condition.condition_id,
  temp,
  feels_like,
  temp_min,
  temp_max,
  pressure,
  humidity,
  sea_level,
  ground_level,
  visibility,
  wind_speed,
  wind_degree,
  wind_gust,
  cloudiness,
  rain_1h,
  rain_3h,
  snow_1h,
  snow_3h
from weather_stage
join {{ ref('dim_datetime') }} as dim_datetime
  on weather_stage.observation_datetime = dim_datetime.date
join {{ ref('dim_weather_condition') }} as dim_weather_condition
  on weather_stage.weather_condition = dim_weather_condition.condition 
  and weather_stage.condition_description = dim_weather_condition.description
{% if is_incremental() %}
where weather_id > (select max(weather_id) from {{ this }})
{% endif %}

