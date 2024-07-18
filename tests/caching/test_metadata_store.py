import pytest

from hamilton.caching.cache_key import create_cache_key
from hamilton.caching.stores.sqlite import SQLiteMetadataStore


@pytest.fixture
def metadata_store(request, tmp_path):
    metdata_store_cls = request.param
    metadata_store = metdata_store_cls(path=tmp_path)
    run_id = "test-run-id"
    try:
        metadata_store.initialize(run_id)
    except BaseException:
        pass

    yield metadata_store

    metadata_store.delete_all()


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_initialize_empty(metadata_store):
    assert metadata_store.size == 0


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_not_empty_after_set(metadata_store):
    code_version = "FOO-1"
    data_version = "foo-a"
    context_key = create_cache_key(code_version=code_version, dep_data_versions={})

    metadata_store.set(
        context_key=context_key,
        node_name="foo",
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )

    assert metadata_store.size > 0


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_set_doesnt_produce_duplicates(metadata_store):
    code_version = "FOO-1"
    data_version = "foo-a"
    context_key = create_cache_key(code_version=code_version, dep_data_versions={})
    metadata_store.set(
        context_key=context_key,
        node_name="foo",
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )
    assert metadata_store.size == 1

    metadata_store.set(
        context_key=context_key,
        node_name="foo",
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )
    assert metadata_store.size == 1


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_get_miss_returns_none(metadata_store):
    context_key = create_cache_key(code_version="FOO-1", dep_data_versions={"bar": "bar-a"})
    data_version = metadata_store.get(context_key=context_key)
    assert data_version is None


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_set_get_without_dependencies(metadata_store):
    code_version = "FOO-1"
    data_version = "foo-a"
    context_key = create_cache_key(code_version=code_version, dep_data_versions={})
    metadata_store.set(
        context_key=context_key,
        node_name="foo",
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )
    retrieved_data_version = metadata_store.get(context_key=context_key)

    assert retrieved_data_version == data_version
