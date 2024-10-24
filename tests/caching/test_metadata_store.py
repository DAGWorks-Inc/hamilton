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
    node_name = "foo"
    cache_key = create_cache_key(
        node_name=node_name, code_version=code_version, dependencies_data_versions={}
    )

    metadata_store.set(
        cache_key=cache_key,
        node_name=node_name,
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )

    assert metadata_store.size > 0


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_set_doesnt_produce_duplicates(metadata_store):
    code_version = "FOO-1"
    data_version = "foo-a"
    node_name = "foo"
    cache_key = create_cache_key(
        node_name=node_name, code_version=code_version, dependencies_data_versions={}
    )
    metadata_store.set(
        cache_key=cache_key,
        node_name=node_name,
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )
    assert metadata_store.size == 1

    metadata_store.set(
        cache_key=cache_key,
        node_name=node_name,
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )
    assert metadata_store.size == 1


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_get_miss_returns_none(metadata_store):
    cache_key = create_cache_key(
        node_name="foo", code_version="FOO-1", dependencies_data_versions={"bar": "bar-a"}
    )
    data_version = metadata_store.get(cache_key=cache_key)
    assert data_version is None


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_set_get_without_dependencies(metadata_store):
    code_version = "FOO-1"
    data_version = "foo-a"
    node_name = "foo"
    cache_key = create_cache_key(
        node_name=node_name, code_version=code_version, dependencies_data_versions={}
    )
    metadata_store.set(
        cache_key=cache_key,
        node_name=node_name,
        code_version=code_version,
        data_version=data_version,
        run_id="...",
    )
    retrieved_data_version = metadata_store.get(cache_key=cache_key)

    assert retrieved_data_version == data_version


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_get_run_ids_returns_ordered_list(metadata_store):
    pre_run_ids = metadata_store.get_run_ids()
    assert pre_run_ids == ["test-run-id"]  # this is from the fixture

    metadata_store.initialize(run_id="foo")
    metadata_store.initialize(run_id="bar")
    metadata_store.initialize(run_id="baz")

    post_run_ids = metadata_store.get_run_ids()
    assert post_run_ids == ["test-run-id", "foo", "bar", "baz"]


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_get_run_results_include_cache_key_and_data_version(metadata_store):
    run_id = "test-run-id"
    metadata_store.set(
        cache_key="foo",
        data_version="1",
        run_id=run_id,
        node_name="a",  # kwarg specific to SQLiteMetadataStore
        code_version="b",  # kwarg specific to SQLiteMetadataStore
    )
    metadata_store.set(
        cache_key="bar",
        data_version="2",
        run_id=run_id,
        node_name="a",  # kwarg specific to SQLiteMetadataStore
        code_version="b",  # kwarg specific to SQLiteMetadataStore
    )

    run_info = metadata_store.get_run(run_id=run_id)

    assert isinstance(run_info, list)
    assert len(run_info) == 2
    assert isinstance(run_info[1], dict)
    assert run_info[0]["cache_key"] == "foo"
    assert run_info[0]["data_version"] == "1"
    assert run_info[1]["cache_key"] == "bar"
    assert run_info[1]["data_version"] == "2"


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_get_run_returns_empty_list_if_run_started_but_no_execution_recorded(metadata_store):
    metadata_store.initialize(run_id="foo")
    run_info = metadata_store.get_run(run_id="foo")
    assert run_info == []


@pytest.mark.parametrize("metadata_store", [SQLiteMetadataStore], indirect=True)
def test_get_run_raises_error_if_run_id_not_found(metadata_store):
    with pytest.raises(IndexError):
        metadata_store.get_run(run_id="foo")
