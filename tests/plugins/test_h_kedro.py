import inspect

import pandas as pd
from kedro.pipeline import node

from hamilton.plugins import h_kedro


def test_parse_k_node_str_output():
    def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
        """Preprocesses the data for companies."""
        companies["iata_approved"] = companies["iata_approved"].astype("category")
        return companies

    kedro_node = node(
        func=preprocess_companies,
        inputs="companies",
        outputs="preprocessed_companies",
        name="preprocess_companies_node",
    )
    h_nodes = h_kedro.k_node_to_h_nodes(kedro_node)
    assert len(h_nodes) == 1
    assert h_nodes[0].name == "preprocessed_companies"
    assert h_nodes[0].type == inspect.signature(preprocess_companies).return_annotation


def test_parse_k_node_list_outputs():
    def multi_outputs() -> dict:
        return dict(a=1, b=2)

    kedro_node = node(
        func=multi_outputs,
        inputs=None,
        outputs=["a", "b"],
    )
    h_nodes = h_kedro.k_node_to_h_nodes(kedro_node)
    node_names = [n.name for n in h_nodes]
    assert len(h_nodes) == 3
    assert "multi_outputs" in node_names
    assert "a" in node_names
    assert "b" in node_names


def test_parse_k_node_dict_outputs():
    def multi_outputs() -> dict:
        return dict(a=1, b=2)

    kedro_node = node(
        func=multi_outputs,
        inputs=None,
        outputs={"a": "a", "b": "b"},
    )
    h_nodes = h_kedro.k_node_to_h_nodes(kedro_node)
    node_names = [n.name for n in h_nodes]
    assert len(h_nodes) == 3
    assert "multi_outputs" in node_names
    assert "a" in node_names
    assert "b" in node_names
