from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition
)

from . import assets

all_assets = load_assets_from_modules([assets])

neo4j_job = define_asset_job("neo4j_job", selection=AssetSelection.all())
neo4j_schedule = ScheduleDefinition(
    job=neo4j_job,
    cron_schedule="*/1 * * * *"
)

defs = Definitions(
    assets=all_assets,
    schedules=[neo4j_schedule]
)
