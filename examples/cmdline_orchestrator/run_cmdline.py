import os

from hamilton.execution.executors import MultiThreadingExecutor, SynchronousLocalTaskExecutor
from hamilton.experimental.h_cache import CachingGraphAdapter
from hamilton import driver

if __name__ == "__main__":
    import funcs
    from cmdline import CMDLineExecutionManager
    from dagworks import adapters

    from hamilton import driver

    tracker = adapters.DAGWorksTracker(
        project_id=19350,
        api_key=os.environ["DAGWORKS_API_KEY"],
        username="lijichen365@gmail.com",
        dag_name="my_version_of_the_dag",
        tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"}
    )

    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_execution_manager(
            CMDLineExecutionManager(SynchronousLocalTaskExecutor(), MultiThreadingExecutor(5))
        )
        .with_modules(funcs)
        .with_adapters(
            tracker,
            CachingGraphAdapter("./cache"),
            #            PrintLnHook()
        )
        .build()
    )
    dr.display_all_functions("graph.dot")
    print(dr.list_available_variables())
    # for var in dr.list_available_variables():
    #     print(dr.execute([var.name], inputs={"start": "hello"}))
    result = dr.execute(["echo"], inputs={"message": "hello"})
    assert result['echo'] == 'hello\n'