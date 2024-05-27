import os

from dagster import Definitions, load_assets_from_modules, define_asset_job, ScheduleDefinition
from .assets import (
  ingestion_assets,
  dbt
)
from .resources import dbt_resource, OpenWeatherMapResource, PostgresResource


"""ASSETS DEFINITIONS"""
dbt_data_assets = load_assets_from_modules(modules=[dbt], group_name="dbt")

all_assets = [
  *ingestion_assets,
  *dbt_data_assets
]


"""JOB DEFINITIONS"""
actual_weather_job = define_asset_job(
  name="actua_weather_job",
  selection=[
    "all_locations",
    "fetch_weather_actual",
    "raw_weather_actual",
    "stage/stg_weather_actual",
    "analytics/dim_datetime",
    "analytics/dim_weather_condition",
    "analytics/fact_weather_actual"
  ]
)

"""SCHEDULE DEFINITIONS"""
hourly_actual_weather_schedule = ScheduleDefinition(
  name="hourly_actual_weather_schedule",
  cron_schedule="0 * * * *",
  job=actual_weather_job
)

"""DEFINITIONS"""
defs = Definitions(
    assets=all_assets,
    resources={
      "dbt": dbt_resource,
      "postgres": PostgresResource(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
      ),
      "open_weather_map": OpenWeatherMapResource(
          api_key=os.getenv("OPEN_WEATHER_MAP_API_KEY")
        )
      },
    jobs=[actual_weather_job],
    schedules=[hourly_actual_weather_schedule]
)
