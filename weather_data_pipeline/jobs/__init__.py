from dagster import (
  define_asset_job
)


"""JOB DEFINITIONS"""
actual_weather_job = define_asset_job(
  name="actual_weather_job",
  selection=[
    "all_locations",
    "fetch_weather_actual",
    "raw_weather_actual",
    "dbt_schema/stage/stg_weather_actual",
    "dbt_schema/analytics/dim_datetime",
    "dbt_schema/analytics/dim_weather_condition",
    "dbt_schema/analytics/fact_weather_actual"
  ],
)

forecast_weather_job = define_asset_job(
  name="forecast_weather_job",
  selection=[
    "all_locations",
    "fetch_weather_forecast",
    "raw_weather_forecast",
    "dbt_schema/stage/stg_weather_forecast",
    "dbt_schema/analytics/dim_datetime",
    "dbt_schema/analytics/dim_weather_condition",
    "dbt_schema/analytics/fact_weather_forecast"
  ],
)

daily_temp_job = define_asset_job(
  name="daily_temp_job",
  selection=[
    "dbt_schema/analytics/fact_daily_temp",
  ]
)

daily_forecast_job = define_asset_job(
  name="daily_forecast_job",
  selection=[
    "dbt_schema/analytics/fact_daily_forecast",
  ]
)
