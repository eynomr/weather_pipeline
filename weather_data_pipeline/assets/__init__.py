from dagster import load_assets_from_package_module
from . import ingestion

ingestion_assets = load_assets_from_package_module(package_module=ingestion, group_name="ingestion")

