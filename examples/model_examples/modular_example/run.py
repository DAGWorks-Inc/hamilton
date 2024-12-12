import pipeline

from hamilton import driver


def run():
    dr = (
        driver.Builder()
        .with_config({"train_model_type": "RandomForest", "model_params": {"n_estimators": 100}})
        .with_modules(pipeline)
        .build()
    )
    dr.display_all_functions("./my_dag.png")
    # dr.execute(["trained_pipeline", "predicted_data"])


if __name__ == "__main__":
    run()
