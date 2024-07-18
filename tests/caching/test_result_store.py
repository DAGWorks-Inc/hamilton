import pathlib
import pickle

import pandas as pd
import pytest

from hamilton.caching import fingerprinting
from hamilton.caching.stores.base import search_data_adapter_registry
from hamilton.caching.stores.file import FileResultStore


@pytest.fixture
def result_store(tmp_path):
    store = FileResultStore(tmp_path / "h-cache")

    yield store

    store.delete_all()


def check_result_store_size(result_store, size: int):
    assert len([p for p in result_store.path.iterdir()]) == size


def test_set(result_store):
    data_version = "foo"
    assert not pathlib.Path(result_store.path, data_version).exists()

    result_store.set(data_version=data_version, result="bar")

    assert pathlib.Path(result_store.path, data_version).exists()
    check_result_store_size(result_store, size=1)


def test_exists(result_store):
    data_version = "foo"
    assert (
        result_store.exists(data_version) == pathlib.Path(result_store.path, data_version).exists()
    )

    result_store.set(data_version=data_version, result="bar")

    assert (
        result_store.exists(data_version) == pathlib.Path(result_store.path, data_version).exists()
    )


def test_set_doesnt_produce_duplicates(result_store):
    data_version = "foo"
    assert not result_store.exists(data_version)

    result_store.set(data_version=data_version, result="bar")
    result_store.set(data_version=data_version, result="bar")

    assert result_store.exists(data_version)
    check_result_store_size(result_store, size=1)


def test_get(result_store):
    data_version = "foo"
    result = "bar"
    pathlib.Path(result_store.path, data_version).open("wb").write(pickle.dumps(result))
    assert result_store.exists(data_version)

    retrieved_value = result_store.get(data_version)

    assert retrieved_value
    assert result == retrieved_value
    check_result_store_size(result_store, size=1)


def test_get_missing_result_is_none(result_store):
    result = result_store.get("foo")
    assert result is None


def test_delete(result_store):
    data_version = "foo"
    result_store.set(data_version, "bar")
    assert pathlib.Path(result_store.path, data_version).exists()
    check_result_store_size(result_store, size=1)

    result_store.delete(data_version)

    assert not pathlib.Path(result_store.path, data_version).exists()
    check_result_store_size(result_store, size=0)


def test_delete_all(result_store):
    result_store.set("foo", "foo")
    result_store.set("bar", "bar")
    check_result_store_size(result_store, size=2)

    result_store.delete_all()

    check_result_store_size(result_store, size=0)


@pytest.mark.parametrize(
    "format,value",
    [
        ("json", {"key1": "value1", "key2": 2}),
        ("pickle", ("value1", "value2", "value3")),
        ("parquet", pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})),
    ],
)
def test_save_and_load_materializer(format, value, result_store):
    saver_cls, loader_cls = search_data_adapter_registry(name=format, type_=type(value))
    data_version = "foo"

    result_store.set(
        data_version=data_version, result=value, saver_cls=saver_cls, loader_cls=loader_cls
    )
    retrieved_value = result_store.get(data_version)

    assert fingerprinting.hash_value(value) == fingerprinting.hash_value(retrieved_value)
