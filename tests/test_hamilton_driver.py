from hamilton.driver import Driver


def test_driver_validate_input_types():
    dr = Driver({'a': 1}, [])
    results = dr.raw_execute(['a'])
    assert results == {'a': 1}
