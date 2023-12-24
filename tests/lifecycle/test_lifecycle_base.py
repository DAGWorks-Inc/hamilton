from typing import Any, Dict, Optional

import pytest

from hamilton import node
from hamilton.graph import FunctionGraph
from hamilton.lifecycle.base import (
    ASYNC_HOOK,
    ASYNC_METHOD,
    REGISTERED_ASYNC_HOOKS,
    REGISTERED_ASYNC_METHODS,
    REGISTERED_SYNC_HOOKS,
    REGISTERED_SYNC_METHODS,
    SYNC_HOOK,
    SYNC_METHOD,
    BaseDoNodeExecute,
    BaseDoValidateInput,
    BasePostGraphExecute,
    BasePreDoAnythingHook,
    InvalidLifecycleAdapter,
    LifecycleAdapterSet,
    lifecycle,
    validate_lifecycle_adapter_function,
)

from tests.lifecycle.lifecycle_adapters_for_testing import ExtendToTrackCalls


def _valid_function_empty():
    pass


def _valid_function_returns_value() -> int:
    return 1


def _valid_function_self(self):
    pass


def _valid_function_self_kwargs(self, *, a: int, b: int) -> int:
    return a + b


@pytest.mark.parametrize(
    "fn, returns_value",
    [
        (_valid_function_empty, False),
        (_valid_function_returns_value, True),
        (_valid_function_self, False),
        (_valid_function_self_kwargs, True),
    ],
)
def test__validate_lifecycle_adapter_function_success(fn, returns_value: bool):
    """Test that the lifecycle adapter function works as expected."""
    validate_lifecycle_adapter_function(fn, returns_value=returns_value)


def _function_with_positional_args(a, b):
    return a + b


def _function_with_mixed_args(self, a, b=None):
    pass


def _function_with_no_return_annotation():
    return 42


def _function_with_return_annotation() -> int:
    return 42


# Test cases


@pytest.mark.parametrize(
    "fn, returns_value",
    [
        (_valid_function_empty, True),
        (_valid_function_returns_value, False),
        (_valid_function_self, True),
        (_valid_function_self_kwargs, False),
        (_function_with_positional_args, True),
        (_function_with_mixed_args, False),
        (_function_with_no_return_annotation, True),
        (_function_with_return_annotation, False),
    ],
)
def test__validate_lifecycle_adapter_function_failure(fn, returns_value: bool):
    """Test that the lifecycle adapter function fails as expected for invalid cases."""
    with pytest.raises(InvalidLifecycleAdapter):
        validate_lifecycle_adapter_function(fn, returns_value=returns_value)


def test_base_hook_decorator():
    @lifecycle.base_hook("hook_for_testing")
    class TestHook:
        def hook_for_testing(self):
            pass

    assert getattr(TestHook, SYNC_HOOK) == "hook_for_testing"
    assert "hook_for_testing" in REGISTERED_SYNC_HOOKS


def test_base_hook_decorator_async():
    @lifecycle.base_hook("async_hook_for_testing")
    class TestHookAsync:
        async def async_hook_for_testing(self):
            pass

    assert getattr(TestHookAsync, ASYNC_HOOK) == "async_hook_for_testing"
    assert "async_hook_for_testing" in REGISTERED_ASYNC_HOOKS


def test_base_method_decorator():
    @lifecycle.base_method("method_for_testing")
    class TestMethod:
        def method_for_testing(self) -> int:
            pass

    assert getattr(TestMethod, SYNC_METHOD) == "method_for_testing"
    assert "method_for_testing" in REGISTERED_SYNC_METHODS


def test_base_method_decorator_async():
    @lifecycle.base_method("async_method_for_testing")
    class TestMethodAsync:
        async def async_method_for_testing(self) -> int:
            pass

    assert getattr(TestMethodAsync, ASYNC_METHOD) == "async_method_for_testing"
    assert "async_method_for_testing" in REGISTERED_ASYNC_METHODS


def test_lifecycle_adapter_set_with_multiple_hooks():
    class MockHook1(BasePreDoAnythingHook, ExtendToTrackCalls):
        def pre_do_anything(self):
            pass

    class MockHook2(BasePreDoAnythingHook, ExtendToTrackCalls):
        def pre_do_anything(self):
            pass

    class MockHook3(BasePostGraphExecute, ExtendToTrackCalls):
        def post_graph_execute(
            self,
            *,
            run_id: str,
            graph: FunctionGraph,
            success: bool,
            error: Optional[Exception],
            results: Optional[Dict[str, Any]]
        ):
            pass

    hook_1 = MockHook1("mock_hook_1")
    hook_2 = MockHook2("mock_hook_2")
    hook_3 = MockHook3("mock_hook_3")

    adapter_set = LifecycleAdapterSet(hook_1, hook_2, hook_3)

    assert adapter_set.does_hook("pre_do_anything", is_async=False)
    assert adapter_set.does_hook("post_graph_execute", is_async=False)
    assert not adapter_set.does_hook("post_graph_execute", is_async=True)
    assert not adapter_set.does_hook("pre_node_execute", is_async=False)
    assert not adapter_set.does_hook("pre_node_execute", is_async=True)

    adapter_set.call_all_lifecycle_hooks_sync("pre_do_anything")
    assert len(hook_1.calls) == 1
    assert len(hook_2.calls) == 1
    assert len(hook_3.calls) == 0


def test_lifecycle_adapter_set_with_single_multi_hook():
    class MockMultiHook(BasePreDoAnythingHook, BasePostGraphExecute, ExtendToTrackCalls):
        def pre_do_anything(self):
            pass

        def post_graph_execute(
            self,
            *,
            run_id: str,
            graph: "FunctionGraph",
            success: bool,
            error: Optional[Exception],
            results: Optional[Dict[str, Any]]
        ):
            pass

    multi_hook = MockMultiHook("mock_multi_hook")
    adapter_set = LifecycleAdapterSet(multi_hook)

    assert adapter_set.does_hook("pre_do_anything", is_async=False)
    assert adapter_set.does_hook("post_graph_execute", is_async=False)
    assert not adapter_set.does_hook("pre_node_execute", is_async=False)
    assert not adapter_set.does_hook("pre_node_execute", is_async=True)

    adapter_set.call_all_lifecycle_hooks_sync("pre_do_anything")
    assert len(multi_hook.calls) == 1  # two pre do anything calls


def test_lifecycle_adapter_set_with_multiple_methods():
    class MockMethod1(BaseDoNodeExecute, ExtendToTrackCalls):
        def do_node_execute(
            self,
            *,
            run_id: str,
            node_: node.Node,
            kwargs: Dict[str, Any],
            task_id: Optional[str] = None
        ) -> Any:
            return 1

    class MockMethod2(BaseDoValidateInput, ExtendToTrackCalls):
        def do_validate_input(self, *, node_type: type, input_value: Any) -> bool:
            return False

    method_1 = MockMethod1("mock_method_1")
    method_2 = MockMethod2("mock_method_2")

    adapter_set = LifecycleAdapterSet(
        method_1,
        method_2,
    )

    assert adapter_set.does_method("do_node_execute", is_async=False)
    assert adapter_set.does_method("do_validate_input", is_async=False)

    assert (
        adapter_set.call_lifecycle_method_sync(
            "do_node_execute", run_id="a", node_=None, kwargs={}, task_id=None
        )
        == 1
    )
    assert len(method_1.calls) == 1
    adapter_set.call_lifecycle_method_sync("do_validate_input", node_type=None, input_value=None)
    assert len(method_2.calls) == 1


def test_lifecycle_adapter_set_with_single_multi_method():
    class MockMultiMethod(BaseDoNodeExecute, BaseDoValidateInput, ExtendToTrackCalls):
        def do_node_execute(
            self,
            *,
            run_id: str,
            node_: node.Node,
            kwargs: Dict[str, Any],
            task_id: Optional[str] = None
        ) -> Any:
            return 1

        def do_validate_input(self, *, node_type: type, input_value: Any) -> bool:
            return True

    multi_hook = MockMultiMethod("mock_multi_hook")
    adapter_set = LifecycleAdapterSet(multi_hook)

    assert adapter_set.does_method("do_node_execute", is_async=False)
    assert adapter_set.does_method("do_validate_input", is_async=False)
    assert not adapter_set.does_method("do_node_execute", is_async=True)

    assert (
        adapter_set.call_lifecycle_method_sync(
            "do_node_execute", run_id="a", node_=None, kwargs={}, task_id=None
        )
        == 1
    )
    assert len(multi_hook.calls) == 1
    adapter_set.call_lifecycle_method_sync("do_validate_input", node_type=None, input_value=None)
    assert len(multi_hook.calls) == 2
