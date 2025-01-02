from concurrent.futures import Future

from hamilton.plugins.h_threadpool import FutureAdapter, _new_fn


def test_new_fn_with_no_futures():
    def sample_fn(a, b):
        return a + b

    result = _new_fn(sample_fn, a=1, b=2)
    assert result == 3


def test_new_fn_with_futures():
    def sample_fn(a, b):
        return a + b

    future_a = Future()
    future_b = Future()
    future_a.set_result(1)
    future_b.set_result(2)

    result = _new_fn(sample_fn, a=future_a, b=future_b)
    assert result == 3


def test_future_adapter_do_remote_execute():
    def sample_fn(a, b):
        return a + b

    adapter = FutureAdapter(max_workers=2)
    future = adapter.do_remote_execute(execute_lifecycle_for_node=sample_fn, node=None, a=1, b=2)
    assert future.result() == 3


def test_future_adapter_do_remote_execute_with_futures():
    def sample_fn(a, b):
        return a + b

    future_a = Future()
    future_b = Future()
    future_a.set_result(1)
    future_b.set_result(2)

    adapter = FutureAdapter(max_workers=2)
    future = adapter.do_remote_execute(
        execute_lifecycle_for_node=sample_fn, node=None, a=future_a, b=future_b
    )
    assert future.result() == 3


def test_future_adapter_build_result():
    adapter = FutureAdapter(max_workers=2)
    future_a = Future()
    future_b = Future()
    future_a.set_result(1)
    future_b.set_result(2)

    result = adapter.build_result(a=future_a, b=future_b)
    assert result == {"a": 1, "b": 2}
