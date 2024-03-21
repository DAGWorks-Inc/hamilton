import os

from hamilton import driver
from hamilton.io.materialization import to
from hamilton.plugins import matplotlib_extensions

import dataflow  # import module with dataflow definition
from .resources import DataGeneratorResource

def main():
    dr = (  
        driver.Builder()
        .with_modules(dataflow)  # pass the module
        .build()
    )
    
    # load environment variable
    num_days = os.environ.get("HACKERNEWS_NUM_DAYS_WINDOW")
    inputs = dict( # mock an API connection
        hackernews_api=DataGeneratorResource(num_days=num_days),
    )
    
    # define I/O operations; decoupled from dataflow def
    materializers = [
        to.json(  # JSON file type
            id="most_frequent_words.json",
            dependencies=["most_frequent_words"],
            path="data/most_frequent_words.json",
        ),
        to.csv(  # CSV file type
            id="topstories.csv",
            dependencies=["topstories"],
            path="data/topstories.csv",
        ),
        to.csv(
            id="signups.csv",
            dependencies=["signups"],
            path="data/signups.csv",
        ),
        to.plt(  # Use matplotlib.pyplot to render
            id="top_25_words_plot.plt",
            dependencies=["top_25_words_plot"],
            path="data/top_25_words_plot.png",
        ),
    ]
    
    # visualize materialization plan without executing code
    dr.visualize_materialization(
        *materializers,
        inputs=inputs,
        output_file_path="dataflow.png"
    )
    # pass I/O operations and inputs to materialize dataflow
    dr.materialize(*materializers, inputs=inputs)
    
if __name__ == "__main__":
    main()
        