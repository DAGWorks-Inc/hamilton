import sys
import textwrap

import pytest

from hamilton import graph_utils


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
        '''
    def A(external_input: int) -> int:
        """This one has a docstring"""
        return external_input % 7
    '''
    )


@pytest.fixture()
def func_a_multiline():
    """The function A() has a docstring"""

    return textwrap.dedent(
        '''
    def A(external_input: int) -> int:
        """This one has
        a multiline
        docstring
        """
        return external_input % 7
    '''
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


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
def test_remove_docstring(func_a: str, func_a_docstring: str):
    func_a_no_whitespace = func_a.strip()
    stripped = graph_utils.remove_docs_and_comments(func_a_docstring)
    assert func_a_no_whitespace == stripped


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
def test_remove_multiline(func_a: str, func_a_multiline: str):
    func_a_no_whitespace = func_a.strip()
    stripped = graph_utils.remove_docs_and_comments(func_a_multiline)
    assert func_a_no_whitespace == stripped


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
def test_remove_comment(func_a: str, func_a_comment: str):
    func_a_no_whitespace = func_a.strip()
    stripped = graph_utils.remove_docs_and_comments(func_a_comment)
    assert func_a_no_whitespace == stripped


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_function_body(func_a: str, func_a_body: str, strip: bool):
    """Gives different hash for different function body"""
    func_a_hash = graph_utils.hash_source_code(func_a, strip=strip)
    func_a_body_hash = graph_utils.hash_source_code(func_a_body, strip=strip)
    assert func_a_hash != func_a_body_hash


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_docstring(func_a: str, func_a_docstring: str, strip: bool):
    """Same hash if strip docstring, else different hash"""
    func_a_hash = graph_utils.hash_source_code(func_a, strip=strip)
    func_a_docstring_hash = graph_utils.hash_source_code(func_a_docstring, strip=strip)
    assert (func_a_hash == func_a_docstring_hash) is (True if strip else False)


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_multiline_docstring(func_a: str, func_a_multiline: str, strip: bool):
    """Same hash if strip multiline docstring, else different hash"""
    func_a_hash = graph_utils.hash_source_code(func_a, strip=strip)
    func_a_multiline_hash = graph_utils.hash_source_code(func_a_multiline, strip=strip)
    assert (func_a_hash == func_a_multiline_hash) is (True if strip else False)


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires Python 3.9 or higher")
@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_comment(func_a: str, func_a_comment: str, strip: bool):
    """Same hash if strip comment, else different hash"""
    func_a_hash = graph_utils.hash_source_code(func_a, strip=strip)
    func_a_comment = graph_utils.hash_source_code(func_a_comment, strip=strip)
    assert (func_a_hash == func_a_comment) is (True if strip else False)
