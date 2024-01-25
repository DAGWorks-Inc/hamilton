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


# onboarding 0
class DocstringValidator(StaticValidator):
    def run_to_validate_node(
        self, *, node: HamiltonNode, **future_kwargs
    ) -> Tuple[bool, Optional[str]]:
        """Validates node documentation -- note that in more sophisticated uses,
        some nodes might be generated internally and you'll likely want to filter those out"""
        docstring = node.documentation
        if len(docstring) == 0:
            return False, f"Node {node.name} has no docstring -- all nodes must have a docstring!"
        return True, None


dr = driver.Builder().with_modules(...).with_adapters(DocstringValidator()).build()


def partition(graph: HamiltonGraph) -> List[List[HamiltonNode]]:
    edges = collections.defaultdict(set)
    name_map = {node.name: node for node in graph.nodes}
    for node in graph.nodes:
        for dependency in node.required_dependencies & node.optional_dependencies:
            edges[dependency].add(node.name)
            edges[node.name].add(dependency)
    node_to_source = {}

    def traverse(node: HamiltonNode, group: str):
        if node.name in node_to_source:
            return  # dfs already completed
        node_to_source[node.name] = group
        for dependency in edges[node.name]:
            traverse(name_map[dependency], group)

    for node in graph.nodes:
        traverse(node, node.name)
    return list(itertools.groupby(sorted(node_to_source.keys()), lambda x: node_to_source[x]))


class GraphIslandValidator(StaticValidator):
    def __init__(self, min_island_size: int = 1):
        self.min_island_size = min_island_size

    def run_to_validate_graph(
        self, graph: HamiltonGraph, **future_kwargs
    ) -> Tuple[bool, Optional[str]]:
        partitions = partition(graph)
        for graph_partition in partitions:
            if len(graph_partition) < self.min_island_size:
                return False, f"Partition {partition} has less than {self.min_island_size} nodes!"


# model_tracking 0


def model(
    model_type: str,
    team: str,
    purpose: str,
    applicable_regions: List[str],
    training_data: Optional[str] = None,
):
    kwargs = {
        "model_type": model_type,
        "team": team,
        "purpose": purpose,
        "applicable_regions": applicable_regions,
        "training_data": training_data,
    }
    return tag(**{key: value for key, value in kwargs.items() if value is not None})


# lineage 0


class LineageHook(NodeExecutionHook, GraphExecutionHook):
    def __init__(self):
        self.lineage_pairs = []
        self.metadata_collection = {}

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
        pass

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        result: Any,
        error: Optional[Exception],
        success: bool,
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        if node_tags.get("hamilton.data_saver", False) or node.tags.get(
            "hamilton.data_loader", False
        ):
            self.metadata_collection[node.name] = {"task_id": task_id, "run_id": run_id}

    def run_before_graph_execution(
        self,
        *,
        graph: graph_types.HamiltonGraph,
        execution_path: Collection[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        data_products = [
            node
            for node in graph.nodes
            if node.tags.get("hamilton.data_saver", False) and node.name in execution_path
        ]
        data_sources = [
            node
            for node in graph.nodes
            if node.tags.get("hamilton.data_loader", False) and node.name in execution_path
        ]
        pairs = []
        for node in data_products:
            linked_sources = dfs(node, search_for=data_sources)
            pairs.extend([(source_node.name, node.name) for source_node in linked_sources])
        self.lineage_pairs = pairs

    def run_after_graph_execution(self, **future_kwargs: Any):
        pass


# migration 0


@dataclasses.dataclass
class BlobLoader(materialization.DataLoader):
    key: str
    project: str
    env: Literal["prod", "dev", "staging"]
    date: datetime
    variant: str = "main"
    callback: Callable = cloudpickle.load

    def __post_init__(self):
        self.bucket = f"{self.env}-artifacts"  # global bucket

    def _get_path(self):
        return f"{self.project}/{self.variant}/{self.date.strftime('%Y-%m-%d')}/{self.key}"

    def load_data(self, type_: Type[Type]) -> Tuple[Type, Dict[str, Any]]:
        client = boto3.client("s3")
        response = client.get_object(
            Bucket=self.project,
            Key=self.key,
        )
        return (
            self.callback(response["Body"]),
            {"path": self._get_path(), "size": response["ContentLength"]},
        )

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [object]

    @classmethod
    def name(cls) -> str:
        return "blob"


# on_call 0


class SlackNotifier(NodeExecutionHook):
    def run_before_node_execution(self, **future_kwargs: Any):
        pass

    def run_after_node_execution(
        self, *, node_kwargs: Dict[str, Any], node_name: str, error: Exception, **future_kwargs: Any
    ):
        """
        Function to be run after a node execution.

        Args:
        node_kwargs (dict): The keyword arguments passed to the node.
        error (Exception, optional): The exception raised during node execution, if any.
        """

        if error is not None:
            node_kwargs_formatted = "\n".join(
                [f"{key}: {value}" for key, value in node_kwargs.items()]
            )
            stack_trace = traceback.format_exc()
            slack_message = (
                f"*Node {node_name} Execution Error!*\n\n"
                f"*Context:*\n"
                "Error occurred during node execution.\n\n"
                f"*Node Arguments:*\n"
                f"```\n{node_kwargs_formatted}\n```\n\n"
                f"*Error Traceback:*\n"
                f"```\n{stack_trace}\n```"
            )
            # Send the message to Slack
            webhook_url = "YOUR_SLACK_WEBHOOK_URL"
            requests.post(webhook_url, json={"text": slack_message})

        # If no error, do nothing
        return

