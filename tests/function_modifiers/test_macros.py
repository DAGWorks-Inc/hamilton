import inspect
from typing import List, Set

import pandas as pd
import pytest

import hamilton.function_modifiers
from hamilton import function_modifiers, models
from hamilton.function_modifiers import does
from hamilton.function_modifiers.macros import ensure_function_empty
from hamilton.node import DependencyType


def test_no_code_validator():
    def no_code():
        pass

    def no_code_with_docstring():
        """This should still show up as having no code, even though it has a docstring"""
        pass

    def yes_code():
        """This should show up as having no code"""
        a = 0
        return a

    ensure_function_empty(no_code)
    ensure_function_empty(no_code_with_docstring)
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        ensure_function_empty(yes_code)


# Functions for  @does -- these are the functions we're "replacing"
def _no_params() -> int:
    pass


def _one_param(a: int) -> int:
    pass


def _two_params(a: int, b: int) -> int:
    pass


def _three_params(a: int, b: int, c: int) -> int:
    pass


def _three_params_with_defaults(a: int, b: int = 1, c: int = 2) -> int:
    pass


def _empty() -> int:
    return 1


def _kwargs(**kwargs: int) -> int:
    return sum(kwargs.values())


def _kwargs_with_a(a: int, **kwargs: int) -> int:
    return a + sum(kwargs.values())


def _just_a(a: int) -> int:
    return a


def _just_b(b: int) -> int:
    return b


def _a_b_c(a: int, b: int, c: int) -> int:
    return a + b + c


@pytest.mark.parametrize(
    "fn,replace_with,argument_mapping,matches",
    [
        (_no_params, _empty, {}, True),
        (_no_params, _kwargs, {}, True),
        (_no_params, _kwargs_with_a, {}, False),
        (_no_params, _just_a, {}, False),
        (_no_params, _a_b_c, {}, False),
        (_one_param, _empty, {}, False),
        (_one_param, _kwargs, {}, True),
        (_one_param, _kwargs_with_a, {}, True),
        (_one_param, _just_a, {}, True),
        (_one_param, _just_b, {}, False),
        (_one_param, _just_b, {"b": "a"}, True),  # Replacing a with b makes the signatures match
        (_one_param, _just_b, {"c": "a"}, False),  # Replacing a with b makes the signatures match
        (_two_params, _empty, {}, False),
        (_two_params, _kwargs, {}, True),
        (_two_params, _kwargs_with_a, {}, True),  # b gets fed to kwargs
        (_two_params, _kwargs_with_a, {"foo": "b"}, True),  # Any kwargs work
        (_two_params, _kwargs_with_a, {"bar": "a"}, False),  # No param bar
        (_two_params, _just_a, {}, False),
        (_two_params, _just_b, {}, False),
        (_three_params, _a_b_c, {}, True),
        (_three_params, _a_b_c, {"d": "a"}, False),
        (_three_params, _a_b_c, {}, True),
        (_three_params, _a_b_c, {"a": "b", "b": "a"}, True),  # Weird case but why not?
        (_three_params, _kwargs_with_a, {}, True),
        (_three_params_with_defaults, _a_b_c, {}, True),
        (_three_params_with_defaults, _a_b_c, {"d": "a"}, False),
        (_three_params_with_defaults, _a_b_c, {}, True),
    ],
)
def test_ensure_function_signatures_compatible(fn, replace_with, argument_mapping, matches):
    assert (
        does.test_function_signatures_compatible(
            inspect.signature(fn), inspect.signature(replace_with), argument_mapping
        )
        == matches
    )


def test_does_function_modifier():
    def sum_(**kwargs: int) -> int:
        return sum(kwargs.values())

    def to_modify(param1: int, param2: int) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(sum_)
    (node_,) = annotation.generate_nodes(to_modify, {})
    assert node_.name == "to_modify"
    assert node_.callable(param1=1, param2=1) == 2
    assert node_.documentation == to_modify.__doc__


def test_does_function_modifier_complex_types():
    def setify(**kwargs: List[int]) -> Set[int]:
        return set(sum(kwargs.values(), []))

    def to_modify(param1: List[int], param2: List[int]) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(setify)
    (node_,) = annotation.generate_nodes(to_modify, {})
    assert node_.name == "to_modify"
    assert node_.callable(param1=[1, 2, 3], param2=[4, 5, 6]) == {1, 2, 3, 4, 5, 6}
    assert node_.documentation == to_modify.__doc__


def test_does_function_modifier_optionals():
    def sum_(param0: int, **kwargs: int) -> int:
        return sum(kwargs.values())

    def to_modify(param0: int, param1: int = 1, param2: int = 2) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(sum_)
    (node_,) = annotation.generate_nodes(to_modify, {})
    assert node_.name == "to_modify"
    assert node_.input_types["param0"][1] == DependencyType.REQUIRED
    assert node_.input_types["param1"][1] == DependencyType.OPTIONAL
    assert node_.input_types["param2"][1] == DependencyType.OPTIONAL
    assert node_.callable(param0=0) == 3
    assert node_.callable(param0=0, param1=0, param2=0) == 0
    assert node_.documentation == to_modify.__doc__


def test_does_with_argument_mapping():
    def _sum_multiply(param0: int, param1: int, param2: int) -> int:
        return param0 + param1 * param2

    def to_modify(parama: int, paramb: int = 1, paramc: int = 2) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(_sum_multiply, param0="parama", param1="paramb", param2="paramc")
    (node_,) = annotation.generate_nodes(to_modify, {})
    assert node_.name == "to_modify"
    assert node_.input_types["parama"][1] == DependencyType.REQUIRED
    assert node_.input_types["paramb"][1] == DependencyType.OPTIONAL
    assert node_.input_types["paramc"][1] == DependencyType.OPTIONAL
    assert node_.callable(parama=0) == 2
    assert node_.callable(parama=0, paramb=1, paramc=2) == 2
    assert node_.callable(parama=1, paramb=4) == 9
    assert node_.documentation == to_modify.__doc__


def test_model_modifier():
    config = {
        "my_column_model_params": {
            "col_1": 0.5,
            "col_2": 0.5,
        }
    }

    class LinearCombination(models.BaseModel):
        def get_dependents(self) -> List[str]:
            return list(self.config_parameters.keys())

        def predict(self, **columns: pd.Series) -> pd.Series:
            return sum(
                self.config_parameters[column_name] * column
                for column_name, column in columns.items()
            )

    def my_column() -> pd.Series:
        """Column that will be annotated by a model"""
        pass

    annotation = function_modifiers.model(LinearCombination, "my_column_model_params")
    annotation.validate(my_column)
    (model_node,) = annotation.generate_nodes(my_column, config)
    assert model_node.input_types["col_1"][0] == model_node.input_types["col_2"][0] == pd.Series
    assert model_node.type == pd.Series
    pd.testing.assert_series_equal(
        model_node.callable(col_1=pd.Series([1]), col_2=pd.Series([2])), pd.Series([1.5])
    )

    def bad_model(col_1: pd.Series, col_2: pd.Series) -> pd.Series:
        return col_1 * 0.5 + col_2 * 0.5

    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        annotation.validate(bad_model)
