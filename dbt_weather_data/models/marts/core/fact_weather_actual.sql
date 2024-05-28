{{
  config(
    materialized='incremental',
    unique_key=['location_id', 'datetime_id'],
    indexes=[
      {'columns': ['location_id', 'datetime_id']}
    ]
  )
}}

with weather_stage as (
  select 
  weather_id,
  location_id,
  case 
    when extract('minute' from observation_datetime) < 30 then date_trunc('hour', observation_datetime)
    else date_trunc('hour', observation_datetime) + interval '30 minutes'
  end as observation_datetime,
  weather_condition,
  condition_description,
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
  snow_3h,
  ingestion_datetime
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
  snow_3h,
  ingestion_datetime
from weather_stage
join {{ ref('dim_datetime') }} as dim_datetime
  on weather_stage.observation_datetime = dim_datetime.date
join {{ ref('dim_weather_condition') }} as dim_weather_condition
  on weather_stage.weather_condition = dim_weather_condition.condition 
  and weather_stage.condition_description = dim_weather_condition.description
{% if is_incremental() %}
where ingestion_datetime > (select max(ingestion_datetime) from {{ this }})
{% endif %}

