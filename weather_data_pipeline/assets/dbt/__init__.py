from typing import Any, Mapping, Optional

from dagster import AssetExecutionContext, AssetKey, file_relative_path
from dagster_dbt import (
  dbt_assets,
  get_asset_key_for_model,
  DbtCliResource,
  DagsterDbtTranslator
)
import os
from pathlib import Path

from ...resources import dbt_resource
from ..constants import DBT_DIRECTORY


if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(
            ['--quiet', 'parse'],
            target_path=Path("target")
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")  


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, any]) -> AssetKey:
        asset_key = super().get_asset_key(dbt_resource_props)

        if dbt_resource_props["resource_type"] == "model":
            asset_key = asset_key.with_prefix(["dbt_schema"])

        return asset_key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        if dbt_resource_props["resource_type"] == "seed":
            return "seed"
        return dbt_resource_props["fqn"][1]

@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(['build'], context=context).stream()

all_locations_asset_key = get_asset_key_for_model([dbt_project_assets], "dim_location")