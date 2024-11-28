from hamilton.driver import Builder, DefaultGraphExecutor


def test_driver_with_local_modules() -> None:
    dr = Builder().with_local_modules().build()
    assert isinstance(dr.graph_executor, DefaultGraphExecutor)
    assert __name__ == list(dr.graph_modules)[0].__name__
