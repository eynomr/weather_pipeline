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
  observation_datetime,
  sunrise,
  sunset,
  ingestion_datetime
 from source

