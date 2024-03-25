from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
    EnvVar,
)

from . import assets
from .resources import DataGeneratorResource

# load assets from passed modules
all_assets = load_assets_from_modules([assets])
# select assets to include in the job
hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

# load environment variable
num_days = EnvVar.int("HACKERNEWS_NUM_DAYS_WINDOW")
defs = Definitions(
    assets=all_assets,
    jobs=[hackernews_job],
    resources={  # register mock API connection
        "hackernews_api": DataGeneratorResource(num_days=num_days),
    },
)