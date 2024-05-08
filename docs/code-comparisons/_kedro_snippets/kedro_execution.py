# run.py
import pandas as pd
from kedro.io import DataCatalog
from kedro.runner import SequentialRunner
from pipeline import create_pipeline
# ^ from Step 2

pipeline = create_pipeline().to_nodes("create_model_input_table")
catalog = DataCatalog(
    feed_dict=dict(
        companies=pd.read_parquet("path/to/companies.parquet"),
        shuttles=pd.read_parquet("path/to/shuttles.parquet"),
    ),
)
SequentialRunner().run(pipeline, catalog)
# doesn't return a value in-memory