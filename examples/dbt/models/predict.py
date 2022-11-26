def model(dbt, session):
    """A DBT model that does a lot -- its all delegated to the hamilton framework though.
    The goal of this is to show how DBT can work for SQL/orchestration, while Hamilton can
    work for workflow modeling (in both the micro/macro sense) and help integrate python in.

    :param dbt:
    :param session:
    :return:
    """
    raw_passengers_df = dbt.ref("raw_passengers").df()
    # This is a quick hack to allow for importing of local libraries
    # This assumes that we're running from within the right directory
    # TODO -- determine a cleaner way to import python modules
    import sys

    sys.path.extend(".")
    # Import our python modules
    import pandas as pd
    from python_transforms import data_loader, feature_transforms, model_pipeline

    from hamilton import base, driver

    adapter = base.SimplePythonGraphAdapter(base.DictResult())
    titanic_dag_loading_previous_model = driver.Driver(
        {
            "random_state": 5,
            "test_size": 0.2,
            "model_to_use": "create_new",
        },
        data_loader,
        feature_transforms,
        model_pipeline,
        adapter=adapter,
    )
    final_vars = [
        "model_predict",
    ]
    results = titanic_dag_loading_previous_model.execute(
        final_vars, inputs={"raw_passengers_df": raw_passengers_df}
    )
    predictions = results["model_predict"]
    return pd.DataFrame(predictions, columns=["prediction"])
