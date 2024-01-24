import analysis
from experiment_hook import ExperimentTracker

from hamilton import driver
from hamilton.io.materialization import to

CACHE_PATH = "/home/tjean/projects/dagworks/hamilton/examples/experiment_management/repository"
EXPERIMENT_PATH = "./experiments"


def main():
    config = dict(
        model="boosting",
        preprocess="pca",
    )

    dr = (
        driver.Builder()
        .with_modules(analysis)
        .with_config(config)
        .with_adapter(
            ExperimentTracker(
                cache_path=CACHE_PATH,  # must be an absolute path; should assert in hook __init__()
                run_dir=EXPERIMENT_PATH,  # accepts a relative path
            )
        )
        .build()
    )

    inputs = dict(favorite_int=9)
    overrides = dict()

    materializers = [
        to.pickle(
            id="trained_model__pickle",
            dependencies=["trained_model"],
            path="./trained_model.pickle",
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
        overrides=overrides,
        output_file_path="dag",
        render_kwargs=dict(format="png", view=False),
    )

    dr.materialize(
        *materializers,
        inputs=inputs,
        overrides=overrides,
    )


if __name__ == "__main__":
    main()
