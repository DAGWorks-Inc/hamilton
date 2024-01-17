from hamilton import graph_types, node


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
