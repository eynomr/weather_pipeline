{{
  config(
    materialized='incremental',
    unique_key=['location_id', 'datetime_id'],
    indexes=[
      {'columns': ['location_id', 'datetime_id']}
    ]
  )
}}

with forecast_stage as (
  select * 
  from {{ ref('stg_weather_forecast') }}
)

select 
  forecast_id,
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
  rain_3h,  
  snow_3h,
  precipitation_probability,
  ingestion_datetime
from forecast_stage
join {{ ref('dim_datetime') }} as dim_datetime
  on forecast_stage.forecast_datetime = dim_datetime.date
join {{ ref('dim_weather_condition') }} as dim_weather_condition
  on forecast_stage.weather_condition = dim_weather_condition.condition 
  and forecast_stage.condition_description = dim_weather_condition.description
{% if is_incremental() %}
where ingestion_datetime > (select max(ingestion_datetime) from {{ this }})
{% endif %}
 