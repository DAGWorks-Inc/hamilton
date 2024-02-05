import analysis
from hamilton_experiments.hook import ExperimentTracker

from hamilton import base, driver
from hamilton.io.materialization import to
from hamilton.plugins import matplotlib_extensions, pandas_extensions  # noqa: F401


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

    inputs = dict(n_splits=3)

    materializers = [
        to.pickle(
            id="trained_model__pickle",
            dependencies=["trained_model"],
            path="./trained_model.pickle",
        ),
        to.parquet(
            id="prediction_df__parquet",
            dependencies=["prediction_df"],
            path="./prediction_df.parquet",
        ),
        to.json(
            id="cv_scores__json",
            dependencies=["cv_scores"],
            combine=base.DictResult(),
            path="./cv_scores.json",
        ),
        to.plt(
            id="prediction_plot__png",
            dependencies=["prediction_plot"],
            path="./prediction_plot.png",
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
