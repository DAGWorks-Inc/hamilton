import pytest

from hamilton import base
from hamilton.driver import Driver
import tests.resources.very_simple_dag
import tests.resources.cyclic_functions
import tests.resources.tagging


def test_driver_validate_input_types():
    dr = Driver({'a': 1})
    results = dr.raw_execute(['a'])
    assert results == {'a': 1}


def test_driver_validate_runtime_input_types():
    dr = Driver({}, tests.resources.very_simple_dag)
    results = dr.raw_execute(['b'], inputs={'a': 1})
    assert results == {'b': 1}


def test_driver_has_cycles_true():
    """Tests that we don't break when detecting cycles from the driver."""
    dr = Driver({}, tests.resources.cyclic_functions)
    assert dr.has_cycles(['C'])

# This is possible -- but we don't want to officially support it. Here for documentation purposes.
# def test_driver_cycles_execute_override():
#     """Tests that we short circuit a cycle by passing in overrides."""
#     dr = Driver({}, tests.resources.cyclic_functions, adapter=base.SimplePythonGraphAdapter(base.DictResult()))
#     result = dr.execute(['C'], overrides={'D': 1}, inputs={'b': 2, 'c': 2})
#     assert result['C'] == 34


def test_driver_cycles_execute_recursion_error():
    """Tests that we throw a recursion error when we try to execute over a DAG that isn't a DAG."""
    dr = Driver({}, tests.resources.cyclic_functions, adapter=base.SimplePythonGraphAdapter(base.DictResult()))
    with pytest.raises(RecursionError):
        dr.execute(['C'], inputs={'b': 2, 'c': 2})

def test_driver_variables():
    dr = Driver({}, tests.resources.tagging)
    tags = {var.name: var.tags for var in dr.list_available_variables()}
    assert tags['a'] == {'module': 'tests.resources.tagging', 'test': 'a'}
    assert tags['b'] == {'module': 'tests.resources.tagging', 'test': 'b_c'}
    assert tags['c'] == {'module': 'tests.resources.tagging', 'test': 'b_c'}
    assert tags['d'] == {'module': 'tests.resources.tagging'}
