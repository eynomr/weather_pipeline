{{
  config(
    materialized='incremental',
    unique_key=['forecast_id', 'location_id', 'datetime_id'],
    indexes=[
      {'columns': ['forecast_id', 'location_id', 'datetime_id']}
    ],
    on_schema_change='sync_all_columns'
  )
}}

with forecast_stage as (
  select 
  location_id::text || '_' || dim_datetime.datetime_id::text as forecast_id,
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
  ingestion_datetime,
  ROW_NUMBER() OVER(PARTITION BY location_id::text || '_' || dim_datetime.datetime_id::text ORDER BY ingestion_datetime DESC) as rn 
  from {{ ref('stg_weather_forecast') }} as stg_weather_forecast
  join {{ ref('dim_datetime') }} as dim_datetime
    on stg_weather_forecast.forecast_datetime = dim_datetime.date
  join {{ ref('dim_weather_condition') }} as dim_weather_condition
    on stg_weather_forecast.weather_condition = dim_weather_condition.condition 
    and stg_weather_forecast.condition_description = dim_weather_condition.description
),
latest_forecast_stage as (
  select *
  from forecast_stage
  where rn = 1
)

select 
  forecast_id,
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
  rain_3h,  
  snow_3h,
  precipitation_probability,
  ingestion_datetime
from latest_forecast_stage
{% if is_incremental() %}
where ingestion_datetime > (select max(ingestion_datetime) from {{ this }})
{% endif %}
 