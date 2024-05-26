from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from ..resources import dbt_resource

import os
from pathlib import Path

from .constants import DBT_DIRECTORY

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

@dbt_assets(
  manifest=dbt_manifest_path
)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(['build'], context=context).stream()