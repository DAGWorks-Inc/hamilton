import analysis
from hamilton_experiments.hook import ExperimentTracker

from hamilton import driver
from hamilton.io.materialization import to
from hamilton.plugins import pandas_extensions  # noqa: F401


def main():
    config = dict(
        model="linear",
        preprocess="pca",
    )

    tracker_hook = ExperimentTracker(
        experiment_name="hello-world",
        base_directory="./experiments",
    )

    dr = (
        driver.Builder()
        .with_modules(analysis)
        .with_config(config)
        .with_adapters(tracker_hook)
        .build()
    )

    inputs = dict(n_splits=4)

    materializers = [
        to.pickle(
            id="trained_model__pickle",
            dependencies=["trained_model"],
            path="./trained_model.pickle",
        ),
        to.parquet(
            id="X_df__parquet",
            dependencies=["X_df"],
            path="./X_df.parquet",
        ),
        to.pickle(
            id="out_of_sample_performance__pickle",
            dependencies=["out_of_sample_performance"],
            path="./out_of_sample_performance.pickle",
        ),
    ]

    dr.visualize_materialization(
        *materializers,
        inputs=inputs,
        output_file_path=f"{tracker_hook.run_directory}/dag",
        render_kwargs=dict(view=False, format="png"),
    )

    dr.materialize(*materializers, inputs=inputs)


if __name__ == "__main__":
    main()
