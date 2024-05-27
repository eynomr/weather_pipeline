from pathlib import Path
from typing import Any, Mapping

from dagster import load_assets_from_package_module
from dagster_dbt import (
  DagsterDbtTranslator,
  DbtCliResource,
  dbt_assets
)
from . import ingestion

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_weather_data").resolve()

ingestion_assets = load_assets_from_package_module(package_module=ingestion, group_name="ingestion")

