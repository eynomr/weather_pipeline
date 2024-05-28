{{
  config(
    materialized='view'
  )
}}

select
weather_id,
forecast_id,
dim_location.city_name as location,
dim_datetime.date as observation_date,
dim_weather_condition_a.condition as actual_condition,
dim_weather_condition_f.condition as forecast_condition,
case when dim_weather_condition_a.condition = dim_weather_condition_f.condition then 1 else 0 end as condition_match,
actual.temp as actual_temp,
forecast.temp as forecast_temp,
abs(actual.temp - forecast.temp) as forecast_error,
1-(abs(actual.temp - forecast.temp)/actual.temp ) as forecast_accuracy_percentage
from {{ ref('fact_weather_actual') }} as actual
join {{ ref('fact_weather_forecast') }} as forecast
  on actual.datetime_id = forecast.datetime_id
join {{ ref('dim_datetime') }} as dim_datetime
  on actual.datetime_id = dim_datetime.datetime_id
join {{ ref('dim_weather_condition') }} as dim_weather_condition_a
  on actual.condition_id = dim_weather_condition_a.condition_id
join {{ ref('dim_weather_condition') }} as dim_weather_condition_f
  on forecast.condition_id = dim_weather_condition_f.condition_id
join {{ ref('dim_location') }} as dim_location
  on actual.location_id = dim_location.location_id

