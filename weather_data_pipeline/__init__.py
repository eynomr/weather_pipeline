import os

from dagster import (
  Definitions
)
from dagster_slack import SlackResource
from .assets import (
  ingestion_assets,
)
from .assets.dbt import (
  dbt_project_assets,
  dbt_resource
)
from .resources import dbt_resource, OpenWeatherMapResource, PostgresResource
from .jobs import actual_weather_job, forecast_weather_job
from .schedules import (
  hourly_actual_weather_schedule,
  daily_forecast_weather_schedule,
  daily_temp_schedule    
)
from .sensors import daily_forecast_sensor, slack_on_run_failure


"""ASSETS DEFINITIONS"""
all_assets = [
  *ingestion_assets,
  dbt_project_assets
]

"""DEFINITIONS"""
defs = Definitions(
    assets=all_assets,
    resources={
      "dbt": dbt_resource,
      "postgres": PostgresResource(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DATABASE", "weather"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
      ),
      "open_weather_map": OpenWeatherMapResource(
          api_key=os.getenv("OPEN_WEATHER_MAP_API_KEY", "api_key")
      ),
      "slack": SlackResource(
        token=os.getenv("SLACK_TOKEN", "token"),        
      )
      },
    jobs=[actual_weather_job, forecast_weather_job],
    schedules=[hourly_actual_weather_schedule, daily_forecast_weather_schedule, daily_temp_schedule],
    sensors=[slack_on_run_failure, daily_forecast_sensor]
)
