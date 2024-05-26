from dagster import asset

@asset
def testing_asset() -> None:
    return None