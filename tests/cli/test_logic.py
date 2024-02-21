from pathlib import Path

from hamilton import driver, graph_types
from hamilton.cli import logic

from tests.cli import module_v1, module_v2


def test_git_directory_exists():
    git_base_dir = logic.get_git_base_directory()

    assert Path(git_base_dir).exists()


def test_map_nodes_to_origins():
    expected_mapping = {
        "customers_path": "customers_df",
        "customers_df": "customers_df",
        "orders_path": "orders_df",
        "orders_df": "orders_df",
        "customers_orders_df": "customers_orders_df",
        "amount": "customers_orders_df",
        "age": "customers_orders_df",
        "country": "customers_orders_df",
        "orders_per_customer": "orders_per_customer",
        "average_order_by_customer": "average_order_by_customer",
        "customer_summary_table": "customer_summary_table",
    }

    dr = driver.Builder().with_modules(module_v1).build()
    graph = graph_types.HamiltonGraph.from_graph(dr.graph)
    node_to_origin = logic.map_nodes_to_functions(graph)

    assert node_to_origin == expected_mapping


def test_diff_versions():
    v1_versions = {
        "average_order_by_customer": "b58a6",
        "customer_summary_table": "6bf52",
        "customers_df": "480be",
        "customers_orders_df": "883f0",
        "orders_df": "58e65",
        "orders_per_customer": "6af6d",
    }
    v2_versions = {
        "average_order_by_customer": "5296f",
        "customer_summary_table": "6bf52",
        "customers_df": "480be",
        "customers_orders_df": "883f0",
        "orders_df": "58e65",
        "orders_per_distributor": "6d64l",
    }

    diff = logic.diff_versions(v1_versions, v2_versions)

    assert diff["v1_only"] == ["orders_per_customer"]
    assert diff["v2_only"] == ["orders_per_distributor"]
    assert diff["edit"] == ["average_order_by_customer"]


def test_diff_node_versions():
    dr1 = driver.Builder().with_modules(module_v1).build()
    dr2 = driver.Builder().with_modules(module_v2).build()

    nodes_v1 = logic.hash_hamilton_nodes(dr1)
    nodes_v2 = logic.hash_hamilton_nodes(dr2)

    diff = logic.diff_versions(nodes_v1, nodes_v2)

    assert diff["v1_only"] == ["orders_per_customer"]
    assert diff["v2_only"] == ["orders_per_distributor"]
    assert diff["edit"] == ["average_order_by_customer", "customer_summary_table"]
