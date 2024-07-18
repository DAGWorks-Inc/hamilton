"""Due to the recursive nature of hashing of sequences, mappings, and other
complex types, many tests are not "true" unit tests. The base cases are
the original `hash_value()` and the `hash_primitive()` functions.
"""

import pytest

from hamilton.caching import fingerprinting


@pytest.mark.parametrize(
    "obj,expected_hash",
    [
        ("hello-world", "IJUxIYl1PeatR9_iDL6X7A=="),
        (17.31231, "vAYX8MD8yEHK6dwnIPVUaw=="),
        (16474, "L_epMRRUy3Qq5foVvFT_OQ=="),
        (True, "-CfPRi9ihI3zfF4elKTadA=="),
        (b"\x951!\x89u=\xe6\xadG\xdf", "qK2VJ0vVTRJemfC0beO8iA=="),
    ],
)
def test_hash_primitive(obj, expected_hash):
    fingerprint = fingerprinting.hash_primitive(obj)
    assert fingerprint == expected_hash


@pytest.mark.parametrize(
    "obj,expected_hash",
    [
        ([0, True, "hello-world"], "Pg9LP3Y-8yYsoWLXedPVKDwTAa7W8_fjJNTTUA=="),
        ((17.0, False, "world"), "wyuuKMuL8rp53_CdYAtyMmyetnTJ9LzmexhJrQ=="),
    ],
)
def test_hash_sequence(obj, expected_hash):
    fingerprint = fingerprinting.hash_sequence(obj)
    assert fingerprint == expected_hash


def test_hash_equals_for_different_sequence_types():
    list_obj = [0, True, "hello-world"]
    tuple_obj = (0, True, "hello-world")
    expected_hash = "Pg9LP3Y-8yYsoWLXedPVKDwTAa7W8_fjJNTTUA=="

    list_fingerprint = fingerprinting.hash_sequence(list_obj)
    tuple_fingerprint = fingerprinting.hash_sequence(tuple_obj)
    assert list_fingerprint == tuple_fingerprint == expected_hash


def test_hash_ordered_mapping():
    obj = {0: True, "key": "value", 17.0: None}
    expected_hash = "a6kiZ3pD0g9vOp1XD_CViVJ9fHYM3ct_oItyJQ=="
    fingerprint = fingerprinting.hash_mapping(obj, ignore_order=False)
    assert fingerprint == expected_hash


def test_hash_mapping_where_order_matters():
    obj1 = {0: True, "key": "value", 17.0: None}
    obj2 = {"key": "value", 17.0: None, 0: True}
    fingerprint1 = fingerprinting.hash_mapping(obj1, ignore_order=False)
    fingerprint2 = fingerprinting.hash_mapping(obj2, ignore_order=False)
    assert fingerprint1 != fingerprint2


def test_hash_unordered_mapping():
    obj = {0: True, "key": "value", 17.0: None}
    expected_hash = "4BCjST4ftDLuBsuNTMgIOOkCy5pV79fCERP9hw=="
    fingerprint = fingerprinting.hash_mapping(obj, ignore_order=True)
    assert fingerprint == expected_hash


def test_hash_mapping_where_order_doesnt_matter():
    obj1 = {0: True, "key": "value", 17.0: None}
    obj2 = {"key": "value", 17.0: None, 0: True}
    fingerprint1 = fingerprinting.hash_mapping(obj1, ignore_order=True)
    fingerprint2 = fingerprinting.hash_mapping(obj2, ignore_order=True)
    assert fingerprint1 == fingerprint2


def test_hash_set():
    obj = {0, True, "key", "value", 17.0, None}
    expected_hash = "4sA1r4wny7AvoG1wzEN6nHdQjpE2V-AodJ9dEQ=="
    fingerprint = fingerprinting.hash_set(obj)
    assert fingerprint == expected_hash
