import pytest

from hamilton.function_modifiers import source, value
from hamilton.function_modifiers.dependencies import LiteralDependency, UpstreamDependency


@pytest.mark.parametrize(
    "upstream_source,expected",
    [("foo", UpstreamDependency("foo")), (UpstreamDependency("bar"), UpstreamDependency("bar"))],
)
def test_upstream(upstream_source, expected):
    assert source(upstream_source) == expected


@pytest.mark.parametrize(
    "literal_value,expected",
    [
        ("foo", LiteralDependency("foo")),
        (LiteralDependency("foo"), LiteralDependency("foo")),
        (1, LiteralDependency(1)),
    ],
)
def test_literal(literal_value, expected):
    assert value(literal_value) == expected
