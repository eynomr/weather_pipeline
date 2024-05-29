{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['forecast_id', 'forecast_datetime', 'ingestion_datetime']}
    ],
  )
}}


with source as (
  select * 
  from {{ source('stage', 'raw_weather_forecast') }}
)

select 
  forecast_id,
  location_id,
  weather_type as weather_condition,
  weather_description as condition_description,
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
  forecast_datetime,
  ingestion_datetime
 from source

