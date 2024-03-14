import inspect
import json
import sys
import textwrap

import pytest

from hamilton import graph_types, node

from tests import nodes as test_nodes


@pytest.fixture()
def func_a():
    """Default function implementation"""

    return textwrap.dedent(
        """
        def A(external_input: int) -> int:
            return external_input % 7
        """
    )


@pytest.fixture()
def func_a_body():
    """The function A() has modulo 5 instead of 7"""

    return textwrap.dedent(
        """
        def A(external_input: int) -> int:
            return external_input % 5
        """
    )


@pytest.fixture()
def func_a_docstring():
    """The function A() has a docstring"""

    return textwrap.dedent(
        """
        def A(external_input: int) -> int:
            '''This one has a docstring'''
            return external_input % 7
        """
    )


@pytest.fixture()
def func_a_multiline():
    """The function A() has a docstring"""

    return textwrap.dedent(
        """
        def A(external_input: int) -> int:
            '''This one has
            a multiline
            docstring
            '''
            return external_input % 7
        """
    )


@pytest.fixture()
def func_a_comment():
    """The function A() has a comment"""

    return textwrap.dedent(
        """
        def A(external_input: int) -> int:
            return external_input % 7  # a comment
        """
    )


def test_create_hamilton_node():
    def node_to_create(required_dep: int, optional_dep: int = 1) -> str:
        """Documentation"""
        return f"{required_dep}_{optional_dep}"

    n = node.Node.from_fn(
        node_to_create
    ).copy_with(  # The following simulate the graph's creation of a node
        tags={"tag_key": "tag_value"}, originating_functions=(node_to_create,)
    )
    hamilton_node = graph_types.HamiltonNode.from_node(n)
    assert hamilton_node.name == "node_to_create"
    assert hamilton_node.type == str
    assert hamilton_node.tags["tag_key"] == "tag_value"
    assert hamilton_node.originating_functions == (node_to_create,)
    assert hamilton_node.documentation == "Documentation"
    assert not hamilton_node.is_external_input
    assert hamilton_node.required_dependencies == {"required_dep"}
    assert hamilton_node.optional_dependencies == {"optional_dep"}
    assert hamilton_node.version == graph_types.hash_source_code(node_to_create, strip=True)

    assert hamilton_node.as_dict() == {
        "name": "node_to_create",
        "tags": {"tag_key": "tag_value"},
        "output_type": "str",
        "required_dependencies": ["required_dep"],
        "optional_dependencies": ["optional_dep"],
        "source": (
            "    def node_to_create(required_dep: int, optional_dep: int = 1) -> str:\n"
            '        """Documentation"""\n'
            '        return f"{required_dep}_{optional_dep}"\n'
        ),
        "documentation": "Documentation",
        "version": graph_types.hash_source_code(node_to_create, strip=True),
    }


def test_json_serializable_dict():
    for name, obj in inspect.getmembers(test_nodes):
        if inspect.isfunction(obj) and not name.startswith("_"):
            n = node.Node.from_fn(
                obj
            ).copy_with(  # The following simulate the graph's creation of a node
                originating_functions=(obj,)
            )
            hamilton_node = graph_types.HamiltonNode.from_node(n)

            # Check that json.dumps works on all nodes
            json.dumps(hamilton_node.as_dict())


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
def test_remove_docstring(func_a: str, func_a_docstring: str):
    func_a_no_whitespace = func_a.strip()
    stripped = graph_types._remove_docs_and_comments(func_a_docstring)
    assert func_a_no_whitespace == stripped


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
def test_remove_multiline(func_a: str, func_a_multiline: str):
    func_a_no_whitespace = func_a.strip()
    stripped = graph_types._remove_docs_and_comments(func_a_multiline)
    assert func_a_no_whitespace == stripped


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
def test_remove_comment(func_a: str, func_a_comment: str):
    func_a_no_whitespace = func_a.strip()
    stripped = graph_types._remove_docs_and_comments(func_a_comment)
    assert func_a_no_whitespace == stripped


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_function_body(func_a: str, func_a_body: str, strip: bool):
    """Gives different hash for different function body"""
    func_a_hash = graph_types.hash_source_code(func_a, strip=strip)
    func_a_body_hash = graph_types.hash_source_code(func_a_body, strip=strip)
    assert func_a_hash != func_a_body_hash


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_docstring(func_a: str, func_a_docstring: str, strip: bool):
    """Same hash if strip docstring, else different hash"""
    func_a_hash = graph_types.hash_source_code(func_a, strip=strip)
    func_a_docstring_hash = graph_types.hash_source_code(func_a_docstring, strip=strip)
    assert (func_a_hash == func_a_docstring_hash) is (True if strip else False)


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_multiline_docstring(func_a: str, func_a_multiline: str, strip: bool):
    """Same hash if strip multiline docstring, else different hash"""
    func_a_hash = graph_types.hash_source_code(func_a, strip=strip)
    func_a_multiline_hash = graph_types.hash_source_code(func_a_multiline, strip=strip)
    assert (func_a_hash == func_a_multiline_hash) is (True if strip else False)


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_comment(func_a: str, func_a_comment: str, strip: bool):
    """Same hash if strip comment, else different hash"""
    func_a_hash = graph_types.hash_source_code(func_a, strip=strip)
    func_a_comment = graph_types.hash_source_code(func_a_comment, strip=strip)
    assert (func_a_hash == func_a_comment) is (True if strip else False)
