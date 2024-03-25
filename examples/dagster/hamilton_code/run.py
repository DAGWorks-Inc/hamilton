import dataflow
from mock_api import DataGeneratorResource

from hamilton import driver
from hamilton.io.materialization import to
from hamilton.plugins import matplotlib_extensions  # noqa: F401


def main():
    dr = driver.Builder().with_modules(dataflow).build()
    dr.display_all_functions("all_functions.png")

    inputs = dict(
        hackernews_api=DataGeneratorResource(num_days=30),
    )

    materializers = [
        to.json(
            id="topstory_ids.json",
            dependencies=["topstory_ids"],
            path="./topstory_ids.json",
        ),
        to.json(
            id="most_frequent_words.json",
            dependencies=["most_frequent_words"],
            path="./most_frequent_words.json",
        ),
        to.csv(
            id="topstories.csv",
            dependencies=["topstories"],
            path="./topstories.csv",
        ),
        to.csv(
            id="signups.csv",
            dependencies=["signups"],
            path="./signups.csv",
        ),
        to.plt(
            id="top_25_words_plot.plt",
            dependencies=["top_25_words_plot"],
            path="./top_25_words_plot.png",
        ),
    ]

    dr.visualize_materialization(*materializers, inputs=inputs, output_file_path="dataflow.png")
    dr.materialize(*materializers, inputs=inputs)


if __name__ == "__main__":
    main()
