import inspect

from hamilton import ad_hoc_utils, function_modifiers


def test_copy_func():
    """Tests that we copy the function as intended"""

    @function_modifiers.tag(test_function="true")
    def foo(bar: int) -> int:
        """dummy function"""
        return bar + 1

    cloned_func = ad_hoc_utils._copy_func(foo)

    assert cloned_func.__dict__ == foo.__dict__
    assert cloned_func.__annotations__ == foo.__annotations__

    assert cloned_func is not foo
    assert cloned_func(1) == foo(1)


def test_generate_unique_temp_module_name():
    """Tests that we replace - with _"""
    name = ad_hoc_utils._generate_unique_temp_module_name()
    assert "-" not in name


def test_create_temporary_module():
    """Tests that we create a module with the passed in functions."""

    def bar(baz: int) -> int:
        """dummy function"""
        return baz + 1

    def foo(bar: int) -> int:
        """dummy function"""
        return bar + 1

    temp_module = ad_hoc_utils.create_temporary_module(bar, foo)
    expected_members = {
        "__spec__",
        "__loader__",
        "__name__",
        "__doc__",
        "bar",
        "foo",
        "__package__",
    }
    assert set(dict(inspect.getmembers(temp_module)).keys()) == expected_members
    assert "_" in temp_module.__name__
    temp_module_2 = ad_hoc_utils.create_temporary_module(bar, foo, module_name="test_module")
    assert set(dict(inspect.getmembers(temp_module_2)).keys()) == expected_members
    assert temp_module_2.__name__ == "test_module"
