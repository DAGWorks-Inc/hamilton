from typing import Any, List, Dict

import pandas as pd
import pytest

from hamilton import function_modifiers, models, function_modifiers_base
from hamilton import node
from hamilton.function_modifiers import does, ensure_function_empty
from hamilton.node import DependencyType


def test_parametrized_invalid_params():
    annotation = function_modifiers.parametrized(
        parameter='non_existant',
        assigned_output={('invalid_node_name', 'invalid_doc'): 'invalid_value'}
    )

    def no_param_node():
        pass

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        annotation.validate(no_param_node)

    def wrong_param_node(valid_value):
        pass

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        annotation.validate(wrong_param_node)


def test_parametrized_single_param_breaks_without_docs():
    with pytest.raises(function_modifiers.InvalidDecoratorException):
        function_modifiers.parametrized(
            parameter='parameter',
            assigned_output={'only_node_name': 'only_value'}
        )


def test_parametrized_single_param():
    annotation = function_modifiers.parametrized(
        parameter='parameter',
        assigned_output={('only_node_name', 'only_doc'): 'only_value'}
    )

    def identity(parameter: Any) -> Any:
        return parameter

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 1
    assert nodes[0].name == 'only_node_name'
    assert nodes[0].type == Any
    assert nodes[0].documentation == 'only_doc'
    called = nodes[0].callable()
    assert called == 'only_value'


def test_parametrized_single_param_expanded():
    annotation = function_modifiers.parametrized(
        parameter='parameter',
        assigned_output={
            ('node_name_1', 'doc1'): 'value_1',
            ('node_value_2', 'doc2'): 'value_2'})

    def identity(parameter: Any) -> Any:
        return parameter

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 2
    called_1 = nodes[0].callable()
    called_2 = nodes[1].callable()
    assert nodes[0].documentation == 'doc1'
    assert nodes[1].documentation == 'doc2'
    assert called_1 == 'value_1'
    assert called_2 == 'value_2'


def test_parametrized_with_multiple_params():
    annotation = function_modifiers.parametrized(
        parameter='parameter',
        assigned_output={
            ('node_name_1', 'doc1'): 'value_1',
            ('node_value_2', 'doc2'): 'value_2'})

    def identity(parameter: Any, static: Any) -> Any:
        return parameter, static

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 2
    called_1 = nodes[0].callable(static='static_param')
    called_2 = nodes[1].callable(static='static_param')
    assert called_1 == ('value_1', 'static_param')
    assert called_2 == ('value_2', 'static_param')


def test_parametrized_input():
    annotation = function_modifiers.parametrized_input(
        parameter='parameter',
        variable_inputs={
            'input_1': ('test_1', 'Function with first column as input'),
            'input_2': ('test_2', 'Function with second column as input')
        })

    def identity(parameter: Any, static: Any) -> Any:
        return parameter, static

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 2
    nodes = sorted(nodes, key=lambda n: n.name)
    assert [n.name for n in nodes] == ['test_1', 'test_2']
    assert set(nodes[0].input_types.keys()) == {'static', 'input_1'}
    assert set(nodes[1].input_types.keys()) == {'static', 'input_2'}


def test_invalid_column_extractor():
    annotation = function_modifiers.extract_columns('dummy_column')

    def no_param_node() -> int:
        pass

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        annotation.validate(no_param_node)


def test_extract_columns_invalid_passing_list_to_column_extractor():
    """Ensures that people cannot pass in a list."""
    with pytest.raises(function_modifiers.InvalidDecoratorException):
        function_modifiers.extract_columns(['a', 'b', 'c'])


def test_extract_columns_empty_args():
    """Tests that we fail on empty arguments."""
    with pytest.raises(function_modifiers.InvalidDecoratorException):
        function_modifiers.extract_columns()


def test_extract_columns_happy():
    """Tests that we are happy with good arguments."""
    function_modifiers.extract_columns(*['a', ('b', 'some doc'), 'c'])


def test_valid_column_extractor():
    """Tests that things work, and that you can provide optional documentation."""
    annotation = function_modifiers.extract_columns('col_1', ('col_2', 'col2_doc'))

    def dummy_df_generator() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame({
            'col_1': [1, 2, 3, 4],
            'col_2': [11, 12, 13, 14]})

    nodes = list(annotation.expand_node(node.Node.from_fn(dummy_df_generator), {}, dummy_df_generator))
    assert len(nodes) == 3
    assert nodes[0] == node.Node(name=dummy_df_generator.__name__, typ=pd.DataFrame, doc_string=dummy_df_generator.__doc__, callabl=dummy_df_generator)
    assert nodes[1].name == 'col_1'
    assert nodes[1].type == pd.Series
    assert nodes[1].documentation == 'dummy doc'  # we default to base function doc.
    assert nodes[1].input_types == {dummy_df_generator.__name__: (pd.DataFrame, DependencyType.REQUIRED)}
    assert nodes[2].name == 'col_2'
    assert nodes[2].type == pd.Series
    assert nodes[2].documentation == 'col2_doc'
    assert nodes[2].input_types == {dummy_df_generator.__name__: (pd.DataFrame, DependencyType.REQUIRED)}


def test_column_extractor_fill_with():
    def dummy_df() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame({
            'col_1': [1, 2, 3, 4],
            'col_2': [11, 12, 13, 14]})

    annotation = function_modifiers.extract_columns('col_3', fill_with=0)
    original_node, extracted_column_node = annotation.expand_node(node.Node.from_fn(dummy_df), {}, dummy_df)
    original_df = original_node.callable()
    extracted_column = extracted_column_node.callable(dummy_df=original_df)
    pd.testing.assert_series_equal(extracted_column, pd.Series([0, 0, 0, 0]), check_names=False)
    pd.testing.assert_series_equal(original_df['col_3'], pd.Series([0, 0, 0, 0]), check_names=False)  # it has to be in there now


def test_column_extractor_no_fill_with():
    def dummy_df_generator() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame({
            'col_1': [1, 2, 3, 4],
            'col_2': [11, 12, 13, 14]})

    annotation = function_modifiers.extract_columns('col_3')
    nodes = list(annotation.expand_node(node.Node.from_fn(dummy_df_generator), {}, dummy_df_generator))
    with pytest.raises(function_modifiers.InvalidDecoratorException):
        nodes[1].callable(dummy_df_generator=dummy_df_generator())


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
    with pytest.raises(function_modifiers.InvalidDecoratorException):
        ensure_function_empty(yes_code)


def test_fn_kwarg_only_validator():
    def kwarg_only(**kwargs):
        pass

    def more_args(param1, param2, *args, **kwargs):
        pass

    def kwargs_and_args(*args, **kwargs):
        pass

    def args_only(*args):
        pass

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        does.ensure_function_kwarg_only(more_args)

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        does.ensure_function_kwarg_only(kwargs_and_args)

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        does.ensure_function_kwarg_only(args_only)

    does.ensure_function_kwarg_only(kwarg_only)


def test_compatible_return_types():
    def returns_int() -> int:
        return 0

    def returns_str() -> str:
        return 'zero'

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        does.ensure_output_types_match(returns_int, returns_str)

    does.ensure_output_types_match(returns_int, returns_int)


def test_does_function_modifier():
    def sum_(**kwargs: int) -> int:
        return sum(kwargs.values())

    def to_modify(param1: int, param2: int) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(sum_)
    node = annotation.generate_node(to_modify, {})
    assert node.name == 'to_modify'
    assert node.callable(param1=1, param2=1) == 2
    assert node.documentation == to_modify.__doc__


def test_model_modifier():
    config = {
        'my_column_model_params': {
            'col_1': .5,
            'col_2': .5,
        }
    }

    class LinearCombination(models.BaseModel):
        def get_dependents(self) -> List[str]:
            return list(self.config_parameters.keys())

        def predict(self, **columns: pd.Series) -> pd.Series:
            return sum(self.config_parameters[column_name] * column for column_name, column in columns.items())

    def my_column() -> pd.Series:
        """Column that will be annotated by a model"""
        pass

    annotation = function_modifiers.model(LinearCombination, 'my_column_model_params')
    annotation.validate(my_column)
    model_node = annotation.generate_node(my_column, config)
    assert model_node.input_types['col_1'][0] == model_node.input_types['col_2'][0] == pd.Series
    assert model_node.type == pd.Series
    pd.testing.assert_series_equal(model_node.callable(col_1=pd.Series([1]), col_2=pd.Series([2])), pd.Series([1.5]))

    def bad_model(col_1: pd.Series, col_2: pd.Series) -> pd.Series:
        return col_1 * .5 + col_2 * .5

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        annotation.validate(bad_model)


def test_sanitize_function_name():
    assert function_modifiers_base.sanitize_function_name('fn_name__v2') == 'fn_name'
    assert function_modifiers_base.sanitize_function_name('fn_name') == 'fn_name'


def test_config_modifier_validate():
    def valid_fn() -> int:
        pass

    def valid_fn__this_is_also_valid() -> int:
        pass

    function_modifiers.config.when(key='value').validate(valid_fn__this_is_also_valid)
    function_modifiers.config.when(key='value').validate(valid_fn)

    def invalid_function__() -> int:
        pass

    with pytest.raises(function_modifiers.InvalidDecoratorException):
        function_modifiers.config.when(key='value').validate(invalid_function__)


def test_config_when():
    def config_when_fn() -> int:
        pass

    annotation = function_modifiers.config.when(key='value')
    assert annotation.resolve(config_when_fn, {'key': 'value'}) is not None
    assert annotation.resolve(config_when_fn, {'key': 'wrong_value'}) is None


def test_config_when_not():
    def config_when_not_fn() -> int:
        pass

    annotation = function_modifiers.config.when_not(key='value')
    assert annotation.resolve(config_when_not_fn, {'key': 'other_value'}) is not None
    assert annotation.resolve(config_when_not_fn, {'key': 'value'}) is None


def test_config_when_in():
    def config_when_in_fn() -> int:
        pass

    annotation = function_modifiers.config.when_in(key=['valid_value', 'another_valid_value'])
    assert annotation.resolve(config_when_in_fn, {'key': 'valid_value'}) is not None
    assert annotation.resolve(config_when_in_fn, {'key': 'another_valid_value'}) is not None
    assert annotation.resolve(config_when_in_fn, {'key': 'not_a_valid_value'}) is None


def test_config_when_not_in():
    def config_when_not_in_fn() -> int:
        pass

    annotation = function_modifiers.config.when_not_in(key=['invalid_value', 'another_invalid_value'])
    assert annotation.resolve(config_when_not_in_fn, {'key': 'invalid_value'}) is None
    assert annotation.resolve(config_when_not_in_fn, {'key': 'another_invalid_value'}) is None
    assert annotation.resolve(config_when_not_in_fn, {'key': 'valid_value'}) is not None


def test_config_name_resolution():
    def fn__v2() -> int:
        pass

    annotation = function_modifiers.config.when(key='value')
    assert annotation.resolve(fn__v2, {'key': 'value'}).__name__ == 'fn'


def test_config_when_with_custom_name():
    def config_when_fn() -> int:
        pass

    annotation = function_modifiers.config.when(key='value', name='new_function_name')
    assert annotation.resolve(config_when_fn, {'key': 'value'}).__name__ == 'new_function_name'


def test_augment_decorator():

    def foo(a: int) -> int:
        return a*2

    annotation = function_modifiers.augment('foo*MULTIPLIER_foo+OFFSET_foo')
    annotation.validate(foo)
    nodes = annotation.transform_dag([node.Node.from_fn(foo)], {}, foo)
    assert 1 == len(nodes)
    nodes_by_name = {node_.name: node_ for node_ in nodes}
    assert set(nodes_by_name) == {'foo'}
    a = 5
    MULTIPLIER_foo = 3
    OFFSET_foo = 7
    foo = a*2
    foo = MULTIPLIER_foo*foo + OFFSET_foo
    assert nodes_by_name['foo'].callable(a=a, MULTIPLIER_foo=MULTIPLIER_foo, OFFSET_foo=OFFSET_foo) == foo # note its foo_raw as that's the node on which it depends
