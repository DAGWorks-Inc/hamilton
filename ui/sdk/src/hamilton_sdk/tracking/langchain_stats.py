"""
Module to pull a few things from langchain objects.
"""

from typing import Any, Dict

from hamilton_sdk.tracking import stats
from langchain_core import documents as lc_documents
from langchain_core import messages as lc_messages


@stats.compute_stats.register(lc_messages.BaseMessage)
def compute_stats_lc_messages(
    result: lc_messages.BaseMessage, node_name: str, node_tags: dict
) -> Dict[str, Any]:
    result = {"value": result.content, "type": result.type}

    return {
        "observability_type": "dict",
        "observability_value": result,
        "observability_schema_version": "0.0.2",
    }


@stats.compute_stats.register(lc_documents.Document)
def compute_stats_lc_docs(
    result: lc_documents.Document, node_name: str, node_tags: dict
) -> Dict[str, Any]:
    if hasattr(result, "to_document"):
        return stats.compute_stats(result.to_document(), node_name, node_tags)
    else:
        # d.page_content  # hack because not all documents are serializable
        result = {"content": result.page_content, "metadata": result.metadata}
    return {
        "observability_type": "dict",
        "observability_value": result,
        "observability_schema_version": "0.0.2",
    }


if __name__ == "__main__":
    # Example usage
    from langchain_core import messages

    msg = messages.BaseMessage(content="Hello, World!", type="greeting")
    print(stats.compute_stats(msg, "greeting", {}))

    doc = lc_documents.Document(page_content="Hello, World!", metadata={"source": "local_dir"})
    print(stats.compute_stats(doc, "document", {}))
