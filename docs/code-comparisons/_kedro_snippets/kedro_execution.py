# run.py
from kedro.runner import SequentialRunner
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pipeline import create_pipeline
# ^ from Step 2

bootstrap_project(".")
with KedroSession.create() as session:
    context = session.load_context()
    catalog = context.catalog

pipeline = create_pipeline().to_nodes("create_model_input_table")
SequentialRunner().run(pipeline, catalog)
# doesn't return values in-memory