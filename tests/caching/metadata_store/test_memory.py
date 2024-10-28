import pytest

from hamilton.caching.stores.memory import InMemoryMetadataStore
from hamilton.caching.stores.sqlite import SQLiteMetadataStore

# `metadata_store` is imported but not directly used because it's
# a pytest fixture automatically provided to tests
from .test_base import _mock_cache_key, metadata_store  # noqa: F401

# implementations that in-memory metadata store can `.persist_to()` and `.load_from()`
PERSISTENT_IMPLEMENTATIONS = [SQLiteMetadataStore]


@pytest.mark.parametrize("metadata_store", PERSISTENT_IMPLEMENTATIONS, indirect=True)
def test_persist_to(metadata_store):  # noqa: F811
    cache_key = _mock_cache_key()
    data_version = "foo-a"
    run_id = "test-run-id"
    in_memory_metadata_store = InMemoryMetadataStore()

    # set values in-memory
    in_memory_metadata_store.initialize(run_id=run_id)
    in_memory_metadata_store.set(
        cache_key=cache_key,
        data_version=data_version,
        run_id=run_id,
    )

    # values exist in memory, but not in destination
    assert in_memory_metadata_store.get(cache_key) == data_version
    assert metadata_store.get(cache_key) is None

    # persist to destination
    in_memory_metadata_store.persist_to(metadata_store)
    assert metadata_store.get(cache_key) == data_version
    assert in_memory_metadata_store.size == metadata_store.size
    assert in_memory_metadata_store.get_run_ids() == metadata_store.get_run_ids()


@pytest.mark.parametrize("metadata_store", PERSISTENT_IMPLEMENTATIONS, indirect=True)
def test_load_from(metadata_store):  # noqa: F811
    cache_key = _mock_cache_key()
    data_version = "foo-a"
    run_id = "test-run-id"

    # set values in source
    metadata_store.initialize(run_id=run_id)
    metadata_store.set(
        cache_key=cache_key,
        data_version=data_version,
        run_id=run_id,
    )

    # values exist in source
    assert metadata_store.get(cache_key) == data_version

    in_memory_metadata_store = InMemoryMetadataStore.load_from(metadata_store)
    assert in_memory_metadata_store.get(cache_key) == data_version
    assert in_memory_metadata_store.size == metadata_store.size
    assert in_memory_metadata_store.get_run_ids() == metadata_store.get_run_ids()
