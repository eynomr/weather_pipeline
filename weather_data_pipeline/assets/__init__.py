from pathlib import Path
from dagster import load_assets_from_package_module
from . import ingestion, core

ingestion_assets = load_assets_from_package_module(package_module=ingestion, group_name="ingestion")
core_assets = load_assets_from_package_module(package_module=core, group_name="core")