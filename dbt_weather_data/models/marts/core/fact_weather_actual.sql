{{
  config(
    materialized='incremental',
    unique_key='weather_id',
    indexes=[
      {'columns': ['weather_id', 'location_id', 'datetime_id']}
    ],
    on_schema_change='sync_all_columns'
  )
}}

with weather_stage as (
  select 
  location_id::text || '_' || dim_datetime.datetime_id::text as weather_id,
    location_id,
    dim_datetime.datetime_id,
    dim_weather_condition.condition_id,
    weather_stage.temp,
    weather_stage.feels_like,
    weather_stage.temp_min,
    weather_stage.temp_max,
    weather_stage.pressure,
    weather_stage.humidity,
    weather_stage.sea_level,
    weather_stage.ground_level,
    weather_stage.visibility,
    weather_stage.wind_speed,
    weather_stage.wind_degree,
    weather_stage.wind_gust,
    weather_stage.cloudiness,
    weather_stage.rain_1h,
    weather_stage.rain_3h,
    weather_stage.snow_1h,
    weather_stage.snow_3h,
    weather_stage.ingestion_datetime,
    row_number() over (partition by location_id::text || '_' || dim_datetime.datetime_id::text 
                       order by ingestion_datetime desc) as rn
    from {{ ref('stg_weather_actual') }} as weather_stage
    join {{ ref('dim_datetime') }} as dim_datetime
      on weather_stage.observation_datetime = dim_datetime.date
    join {{ ref('dim_weather_condition') }} as dim_weather_condition
      on weather_stage.weather_condition = dim_weather_condition.condition 
      and weather_stage.condition_description = dim_weather_condition.description
)
, latest_weather_stage as (
  select *
  from weather_stage
  where rn = 1
)

select 
  weather_id,
  location_id,
  datetime_id,
  condition_id,
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
from latest_weather_stage
{% if is_incremental() %}
where ingestion_datetime > (select max(ingestion_datetime) from {{ this }})
{% endif %}