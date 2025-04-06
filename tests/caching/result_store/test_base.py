import pytest

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
def result_store(request, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("result_store")
    result_store_cls = request.param
    result_store = _instantiate_result_store(result_store_cls, tmp_path)

    yield result_store

    result_store.delete_all()


# NOTE add tests that check properties shared across result store implementations below
