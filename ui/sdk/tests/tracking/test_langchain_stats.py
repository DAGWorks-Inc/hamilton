from hamilton_sdk.tracking import langchain_stats
from langchain_core import documents as lc_documents
from langchain_core import messages as lc_messages


def test_compute_stats_lc_docs():
    result = lc_documents.Document(page_content="Hello, World!", metadata={"source": "local_dir"})
    node_name = "test_node"
    node_tags = {}
    actual = langchain_stats.compute_stats_lc_docs(result, node_name, node_tags)
    expected = {
        "observability_schema_version": "0.0.2",
        "observability_type": "dict",
        "observability_value": {"content": "Hello, World!", "metadata": {"source": "local_dir"}},
    }
    assert actual == expected


def test_compute_stats_lc_messages():
    result = lc_messages.AIMessage(content="Hello, World!")
    node_name = "test_node"
    node_tags = {}
    actual = langchain_stats.compute_stats_lc_messages(result, node_name, node_tags)
    expected = {
        "observability_schema_version": "0.0.2",
        "observability_type": "dict",
        "observability_value": {"type": "ai", "value": "Hello, World!"},
    }
    assert actual == expected
