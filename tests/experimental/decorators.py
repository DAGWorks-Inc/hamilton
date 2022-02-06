from hamilton import node
from hamilton.experimental.decorators import augment


def test_augment_decorator():

    def foo(a: int) -> int:
        return a*2

    annotation = augment('foo*MULTIPLIER_foo+OFFSET_foo')
    annotation.validate(foo)
    nodes = annotation.transform_dag([node.Node.from_fn(foo)], {}, foo)
    assert 1 == len(nodes)
    nodes_by_name = {node_.name: node_ for node_ in nodes}
    assert set(nodes_by_name) == {'foo'}
    a = 5
    MULTIPLIER_foo = 3
    OFFSET_foo = 7
    foo = a*2
    foo = MULTIPLIER_foo*foo + OFFSET_foo
    assert nodes_by_name['foo'].callable(a=a, MULTIPLIER_foo=MULTIPLIER_foo, OFFSET_foo=OFFSET_foo) == foo # note its foo_raw as that's the node on which it depends
