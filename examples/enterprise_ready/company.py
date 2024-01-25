from typing import Dict

from examples.enterprise_ready.team import gather_ec2_type, get_cost_per_hour
from hamilton.lifecycle import GraphExecutionHook

import collections
import dataclasses
import importlib.metadata
import itertools
import traceback
from datetime import datetime
from typing import Any, Callable, Collection, Dict, List, Literal, Optional, Tuple, Type

import boto3 as boto3
import requests
from cloudpickle import cloudpickle

from hamilton import driver, graph_types
from hamilton.function_modifiers import tag
from hamilton.graph_types import HamiltonGraph, HamiltonNode
from hamilton.io import materialization
from hamilton.lifecycle import GraphExecutionHook, NodeExecutionHook, StaticValidator


def dfs(*args, **kwargs):
    pass


def contains_pii(uri: str) -> bool:
    pass


def mark_as_containing_pii(uri: str) -> None:
    pass


def track_dataset(*args, **kwargs) -> None:
    pass


def track_model(*args, **kwargs) -> None:
    pass


def extract_type(result: Any) -> str:
    pass


def extract_uri(result: Any) -> str:
    pass


def gather_ec2_type() -> str:
    pass


def get_cost_per_hour(ec2_type: str) -> float:
    pass



# compliance 0


def anonymized(fn):
    """Represents an anonymizing operation -- E.G. one that removes PII from a dataset"""
    return tag(compliance_properties=["no_pii"])(fn)


# compliance 1


class PIITracker(NodeExecutionHook, GraphExecutionHook):
    def __init__(self):
        self.output_map = {}  # track the nodes that are associated with output data
        self.input_map = {}  # track the nodes that are associated with input data

    def run_after_node_execution(
        self, *, node_name: str, node_tags: Dict[str, Any], result: Any, **future_kwargs: Any
    ):
        if node_tags.get("hamilton.data_saver", False):
            self.output_map[node_name] = extract_uri(result)
        elif node_tags.get("hamilton.data_loader", False):
            # Note this uses an internal API -- possible this might change slightly in the future
            _, metadata = result  # Tuple of data, metadata
            self.input_map[node_name] = extract_uri(metadata)

    def run_after_graph_execution(
        self,
        *,
        graph: graph_types.HamiltonGraph,
        results: Optional[Dict[str, Any]],
        **future_kwargs: Any,
    ):
        all_nodes_by_name = {node.name: node for node in graph.nodes}
        nodes_with_pii = set()
        nodes_without_pii = set()

        def dfs(node: HamiltonNode):
            if node.name in nodes_with_pii or node.name in nodes_without_pii:
                # already visited
                return
            if "no_pii" in node.tags.get("compliance_properties", []):
                nodes_without_pii.add(node.name)
                return  # no need to traverse further
            if node.name in self.input_map and contains_pii(self.input_map[node.name]):
                nodes_with_pii.add(node.name)
                return
            for dependency in node.required_dependencies:
                dfs(all_nodes_by_name[dependency])
                if dependency in nodes_with_pii:
                    nodes_with_pii.add(node.name)
                    return

        for result in self.output_map:
            dfs(all_nodes_by_name[result])
        outputs_with_pii = [self.output_map[node] for node in nodes_with_pii]
        for output in outputs_with_pii:
            # TODO -- send this to your compliance team
            mark_as_containing_pii(output)


# dataset_tracking 0


class DataSetTracker(NodeExecutionHook):
    def run_after_node_execution(
        self, *, node_name: str, node_tags: Dict[str, Any], result, **future_kwargs: Any
    ):
        if node_tags.get("hamilton.data_saver", False):
            artifact_type = extract_type(result)
            artifact_uri = extract_uri(result)
            track_dataset(uri=artifact_uri, name=node_name, last_written=datetime.now())


# model_tracking 0
class ModelTracker(NodeExecutionHook):
    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_type: Type,
        result,
        **future_kwargs: Any,
    ):
        if node_tags.get("hamilton.data_saver", False):
            artifact_type = extract_type(result)
            artifact_uri = extract_uri(result)
            if artifact_type == "model":
                track_model(
                    uri=artifact_uri,
                    name=node_name,
                    last_written=datetime.now(),
                    model_type=str(node_type),
                )


# security 0


class VulnerabilityTracker(GraphExecutionHook):
    def gather_vulnerabilities(self) -> Dict[str, Any]:
        library_data = [
            {
                "package": {"ecosystem": "PyPI", "name": dist.metadata["Name"]},
                "version": dist.version,
            }
            for dist in importlib.metadata.distributions()
        ]
        with_vulnerabilities = {}
        url = "https://api.osv.dev/v1/querybatch"
        data = {"queries": library_data}
        response = requests.post(url, json=data)
        response_data = response.json()
        for i, result in enumerate(response_data["results"]):
            if len(result["vulns"]) > 0:
                with_vulnerabilities[library_data[i]["package"]["name"]] = library_data[i][
                    "version"
                ]
        return with_vulnerabilities

    def run_before_graph_execution(self, **future_kwargs: Any):
        vulnerabilities = self.gather_vulnerabilities()
        if len(vulnerabilities) > 0:
            # TODO -- send this to your security team
            raise Exception(
                f"Vulnerabilities found: {vulnerabilities}. "
                "Please fix before running graph! "
                "Thank you for keeping us safe."
            )


# cost_estimation 0


class CostEstimator(GraphExecutionHook, NodeExecutionHook):
    def __init__(self):
        self.ec2_type = gather_ec2_type()
        self.cost_per_hour = get_cost_per_hour(self.ec2_type)
        self.node_start_times = {}
        self.node_costs = {}

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        self.node_times[node_name] = datetime.now()

    def run_after_node_execution(self, *, node_name: str, **future_kwargs):
        node_time = (datetime.now() - self.node_times[node_name]).total_seconds()
        cost = node_time * self.cost_per_hour / 3600
        self.node_costs[node_name] = cost

    def run_after_graph_execution(self, **future_kwargs: Any):
        for executed_node in self.node_costs:
            print(f"node: {executed_node} cost: ${self.node_costs[executed_node]}")
