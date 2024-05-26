import os

from dagster import Definitions, load_assets_from_modules
from .assets import (
  ingestion_assets,
  core_assets,
  dbt
)
from .resources import dbt_resource, OpenWeatherMapResource, PostgresResource

dbt_data_assets = load_assets_from_modules(modules=[dbt])

all_assets = [
  *ingestion_assets,
  *core_assets,
  *dbt_data_assets
]

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
)
