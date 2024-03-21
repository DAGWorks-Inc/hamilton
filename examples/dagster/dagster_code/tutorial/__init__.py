from dagster import AssetSelection, Definitions, EnvVar, define_asset_job, load_assets_from_modules

from . import assets
from .resources import DataGeneratorResource

all_assets = load_assets_from_modules([assets])

hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

datagen = DataGeneratorResource(num_days=EnvVar.int("HACKERNEWS_NUM_DAYS_WINDOW"))

defs = Definitions(
    assets=all_assets,
    jobs=[hackernews_job],
    resources={
        "hackernews_api": datagen,
    },
)
