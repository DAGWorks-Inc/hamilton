from hamilton.driver import Driver
import tests.resources.very_simple_dag


def test_driver_validate_input_types():
    dr = Driver({'a': 1})
    results = dr.raw_execute(['a'])
    assert results == {'a': 1}


def test_driver_validate_runtime_input_types():
    dr = Driver({}, tests.resources.very_simple_dag)
    results = dr.raw_execute(['b'], inputs={'a': 1})
    assert results == {'b': 1}
