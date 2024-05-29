{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['weather_id', 'observation_datetime', 'ingestion_datetime']}
    ],
  )
}}

with source as (
  select * 
  from {{ source('stage', 'raw_weather_actual') }}
)

select 
  weather_id,
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
  rain_1h,
  rain_3h,
  snow_1h,
  snow_3h,
  case 
    when extract('minute' from observation_datetime) < 30 then date_trunc('hour', observation_datetime)
    else date_trunc('hour', observation_datetime) + interval '30 minutes'
  end as observation_datetime,
  sunrise,
  sunset,
  ingestion_datetime
 from source

