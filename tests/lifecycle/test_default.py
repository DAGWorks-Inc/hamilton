import sys
from typing import Any, Dict, List, Union

import pytest

from hamilton.lifecycle.default import check_instance


def test_check_instance_with_non_generic_type():
    assert check_instance(5, int)
    assert not check_instance("5", int)


def test_check_instance_with_generic_list_type():
    assert check_instance([1, 2, 3], List[int])
    assert check_instance([1, 2, 3], List)
    assert not check_instance([1, 2, "3"], List[int])
    assert check_instance([1, 2, "3"], List)


def test_check_instance_with_list_type():
    assert check_instance([1, 2, 3], list)
    assert check_instance([1, 2, "3"], list)


def test_check_instance_with_generic_dict_type():
    assert check_instance({"key1": 1, "key2": 2}, Dict[str, int])
    assert check_instance({"key1": 1, "key2": 2}, Dict)
    assert not check_instance({"key1": 1, "key2": "2"}, Dict[str, int])
    assert check_instance({"key1": 1, "key2": "2"}, Dict)


def test_check_instance_with_dict_type():
    assert check_instance({"key1": 1, "key2": 2}, dict)
    assert check_instance({"key1": 1, "key2": "2"}, dict)


def test_check_instance_with_nested_generic_type():
    assert check_instance([{"key1": 1, "key2": 2}, {"key3": 3, "key4": 4}], List[Dict[str, int]])
    assert not check_instance(
        [{"key1": 1, "key2": 2}, {"key3": 3, "key4": "4"}], List[Dict[str, int]]
    )


def test_check_instance_with_none_type():
    assert check_instance(None, type(None))
    assert not check_instance(5, type(None))


def test_check_instance_with_any_type():
    assert check_instance(5, Any)
    assert check_instance("5", Any)
    assert check_instance([1, 2, 3], Any)
    assert check_instance({"key1": 1, "key2": 2}, Any)


def test_check_instance_with_union_type():
    assert check_instance(5, Union[int, str])
    assert check_instance("5", Union[int, str])
    assert not check_instance([1, 2, 3], Union[int, str])
    assert not check_instance({"key1": 1, "key2": 2}, Union[int, str])


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires python 3.9 or higher")
def test_check_instance_with_union_type_and_literal():
    from typing import Literal

    assert check_instance("a", Union[Literal["a"], Literal["b"]])
    assert check_instance("b", Union[Literal["a"], Literal["b"]])
    assert not check_instance("c", Union[Literal["a"], Literal["b"]])


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires python 3.9 or higher")
def test_non_generic_dict_and_list():
    assert check_instance([1, 2, 3], list[int])
    assert not check_instance([1, 2, "3"], list[int])
    assert check_instance({"key1": 1, "key2": 2}, dict[str, int])
    assert not check_instance({"key1": 1, "key2": "2"}, dict[str, int])


def test_with_random_object():
    class RandomObject:
        pass

    assert check_instance(RandomObject(), RandomObject)
    assert not check_instance(RandomObject(), int)
    assert not check_instance(RandomObject(), list)
    assert not check_instance(RandomObject(), dict)
    assert check_instance(RandomObject(), Any)
    assert check_instance(RandomObject(), Union[RandomObject, int])
    assert check_instance(RandomObject(), Union[RandomObject, int, str])
    assert not check_instance(RandomObject(), Union[int, str])
    assert not check_instance(RandomObject(), Union[int, str, list])
    assert not check_instance(RandomObject(), Union[int, str, dict])
    assert not check_instance(RandomObject(), Union[int, str, list, dict])
    assert not check_instance(RandomObject(), Union[int, str, list, dict, None])
    assert check_instance(RandomObject(), Union[int, str, list, dict, RandomObject])
    assert not check_instance(RandomObject(), Union[int, str, list, dict, None, float])
    assert check_instance(RandomObject(), Union[int, str, list, dict, None, RandomObject])
    assert check_instance(RandomObject(), Union[int, str, list, dict, None, RandomObject, float])
    assert not check_instance(RandomObject(), Union[int, str, list, dict, None, float, bool])
    assert check_instance(
        RandomObject(), Union[int, str, list, dict, None, RandomObject, float, bool]
    )
    assert not check_instance(RandomObject(), Union[int, str, list, dict, None, float, bool, bytes])
    assert check_instance(
        RandomObject(), Union[int, str, list, dict, None, RandomObject, float, bool, bytes]
    )
    assert not check_instance(
        RandomObject(), Union[int, str, list, dict, None, float, bool, bytes, complex]
    )
    assert check_instance(
        RandomObject(), Union[int, str, list, dict, None, RandomObject, float, bool, bytes, complex]
    )
    assert not check_instance(
        RandomObject(), Union[int, str, list, dict, None, float, bool, bytes, complex]
    )
    assert check_instance(
        RandomObject(), Union[int, str, list, dict, None, RandomObject, float, bool, bytes, complex]
    )
    assert not check_instance(
        RandomObject(), Union[int, str, list, dict, None, float, bool, bytes, complex, type(None)]
    )
