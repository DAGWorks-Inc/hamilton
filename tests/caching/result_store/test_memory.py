import pytest

from hamilton.caching.stores.base import StoredResult
from hamilton.caching.stores.file import FileResultStore
from hamilton.caching.stores.memory import InMemoryResultStore


def _instantiate_result_store(result_store_cls, tmp_path):
    if result_store_cls == FileResultStore:
        return FileResultStore(path=tmp_path)
    elif result_store_cls == InMemoryResultStore:
        return InMemoryResultStore()
    else:
        raise ValueError(
            f"Class `{result_store_cls}` isn't defined in `_instantiate_metadata_store()`"
        )


@pytest.fixture
def result_store(request, tmp_path):
    result_store_cls = request.param
    result_store = _instantiate_result_store(result_store_cls, tmp_path)

    yield result_store

    result_store.delete_all()


def _store_size(result_store: InMemoryResultStore) -> int:
    return len(result_store._results)


@pytest.fixture
def memory_store():
    store = InMemoryResultStore()
    assert _store_size(store) == 0
    yield store


def test_set(memory_store):
    data_version = "foo"

    memory_store.set(data_version=data_version, result="bar")

    assert memory_store._results[data_version].value == "bar"
    assert _store_size(memory_store) == 1


def test_exists(memory_store):
    data_version = "foo"
    assert memory_store.exists(data_version) is False

    memory_store.set(data_version=data_version, result="bar")

    assert memory_store.exists(data_version) is True


def test_set_doesnt_produce_duplicates(memory_store):
    data_version = "foo"
    assert not memory_store.exists(data_version)

    memory_store.set(data_version=data_version, result="bar")
    memory_store.set(data_version=data_version, result="bar")

    assert memory_store.exists(data_version)
    assert _store_size(memory_store) == 1


def test_get(memory_store):
    data_version = "foo"
    result = StoredResult(value="bar")
    memory_store._results[data_version] = result
    assert memory_store.exists(data_version)

    retrieved_value = memory_store.get(data_version)

    assert retrieved_value is not None
    assert result.value == retrieved_value
    assert _store_size(memory_store) == 1


def test_get_missing_result_is_none(memory_store):
    result = memory_store.get("foo")
    assert result is None


def test_delete(memory_store):
    data_version = "foo"
    memory_store._results[data_version] = StoredResult(value="bar")
    assert _store_size(memory_store) == 1

    memory_store.delete(data_version)

    assert memory_store._results.get(data_version) is None
    assert _store_size(memory_store) == 0


def test_delete_all(memory_store):
    memory_store._results["foo"] = "foo"
    memory_store._results["bar"] = "bar"
    assert _store_size(memory_store) == 2

    memory_store.delete_all()

    assert _store_size(memory_store) == 0


@pytest.mark.parametrize(
    "result_store",
    [FileResultStore],
    indirect=True,
)
def test_persist_to(result_store, memory_store):  # noqa: F811
    data_version = "foo"
    result = "bar"

    # set values in-memory
    memory_store.set(data_version=data_version, result=result)

    # values exist in memory, but not in destination
    assert memory_store.get(data_version) == result
    assert result_store.get(data_version) is None

    # persist to destination
    memory_store.persist_to(result_store)
    assert memory_store.get(data_version) == result_store.get(data_version)


@pytest.mark.parametrize(
    "result_store",
    [FileResultStore],
    indirect=True,
)
def test_load_from(result_store):  # noqa: F811
    data_version = "foo"
    result = "bar"

    # set values in source
    result_store.set(data_version=data_version, result=result)

    # values exist in source
    assert result_store.get(data_version) == result

    memory_store = InMemoryResultStore.load_from(
        result_store=result_store, data_versions=[data_version]
    )
    assert memory_store.get(data_version) == result_store.get(data_version)


def test_load_from_must_have_metadata_store_or_data_versions(tmp_path):
    result_store = FileResultStore(tmp_path)
    with pytest.raises(ValueError):
        InMemoryResultStore.load_from(result_store)
