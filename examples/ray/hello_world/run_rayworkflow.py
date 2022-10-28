import ray
from ray import workflow

from hamilton import base, driver, log_setup
from hamilton.experimental import h_ray

if __name__ == "__main__":
    log_setup.setup_logging()
    workflow.init()
    # You can also script module import loading by knowing the module name
    # See run.py for an example of doing it that way.
    import business_logic
    import data_loaders

    modules = [data_loaders, business_logic]
    initial_columns = {  # could load data here via some other means, or delegate to a module as we have done.
        # 'signups': pd.Series([1, 10, 50, 100, 200, 400]),
        "signups_location": "some_path",
        # 'spend': pd.Series([10, 10, 20, 40, 40, 50]),
        "spend_location": "some_other_path",
    }
    rga = h_ray.RayWorkflowGraphAdapter(
        result_builder=base.PandasDataFrameResult(),
        # Ray will resume a run if possible based on workflow id
        workflow_id="hello-world-123",
    )
    dr = driver.Driver(initial_columns, *modules, adapter=rga)
    # we need to specify what we want in the final dataframe.
    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
        "spend_zero_mean_unit_variance",
    ]
    # let's create the dataframe!
    df = dr.execute(output_columns)
    # To visualize do `pip install sf-hamilton[visualization]` if you want these to work
    # dr.visualize_execution(output_columns, './my_dag.dot', {})
    # dr.display_all_functions('./my_full_dag.dot')
    print(df.to_string())
    ray.shutdown()
