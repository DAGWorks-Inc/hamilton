from hamilton import base, driver, graph
from hamilton.graph_utils import find_functions
from tests.resources.generators import degenerate_linear, sequential_diamond, sequential_linear

ADAPTER = base.SimplePythonGraphAdapter(base.DictResult())


def test_degenerate_linear_end_to_end():
    dr = driver.Driver({}, degenerate_linear, adapter=ADAPTER)
    assert dr.execute(["final"])["final"] == degenerate_linear._calc()


def test_sequential_linear_end_to_end():
    dr = driver.Driver({}, sequential_linear, adapter=ADAPTER)
    assert dr.execute(["final"])["final"] == sequential_linear._calc()


def test_sequential_diamond_end_to_end():
    dr = driver.Driver({}, sequential_diamond, adapter=ADAPTER)
    assert dr.execute(["final"])["final"] == sequential_diamond._calc()


def test_sequential_linear_group_nodes():
    nodes = graph.gather_nodes(
        find_functions(sequential_linear), {}, [base.SimplePythonDataFrameGraphAdapter()]
    )
    assert len(nodes) == 5
    groups = graph.group_nodes(nodes)
    print("\n".join([item.to_tree_repr() for item in groups]))
    # print("\n" + root_group.to_tree_repr())
    assert len(groups) == 3  # One for the precursor, one for the repeat, and one for the collect
    fn_graph = graph.FunctionGraph(groups, [], {})
    results = fn_graph.execute(["final"])
    print(results)
    import pdb

    pdb.set_trace()
