import dbm
import hashlib
import inspect
import json
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from hamilton import graph_types, lifecycle


class JsonCache:
    def __init__(self, cache_path: str):
        self.cache_path = f"{cache_path}/json_cache"

    def keys(self) -> list:
        with dbm.open(self.cache_path, "r") as db:
            return list(db.keys())

    def write(self, data: str, id_: str) -> None:
        with dbm.open(self.cache_path, "c") as db:
            db[id_] = data

    def read(self, id_: str) -> str:
        with dbm.open(self.cache_path, "r") as db:
            return db[id_].decode()

    def delete(self, id_: str) -> None:
        with dbm.open(self.cache_path, "r") as db:
            del db[id_]


def _get_default_input(node) -> Any:
    """Get default input node value from originating function signature"""
    param_name = node.name
    origin_function = node.originating_functions[0]
    param = inspect.signature(origin_function).parameters[param_name]
    return None if param.default is inspect._empty else param.default


def json_encoder(obj: Any):
    if isinstance(obj, set):
        serialized = list(obj)
    else:
        obj_hash = hashlib.sha256()
        obj_hash.update(obj)
        serialized = dict(
            dtype=type(obj).__name__,
            obj_hash=obj_hash.hexdigest(),
        )
    return serialized


def graph_hash(graph: graph_types.HamiltonGraph) -> str:
    nodes_data = []
    for node in graph.nodes:
        source_code = ""
        if node.originating_functions is not None:
            source_code = inspect.getsource(node.originating_functions[0])

        nodes_data.append(dict(name=node.name, source_code=source_code))

    digest = hashlib.sha256()
    digest.update(json.dumps(nodes_data, default=json_encoder, sort_keys=True).encode())
    return digest.hexdigest()


# matches pydantic_models.py
@dataclass
class NodeImplementation:
    name: str
    source_code: str


@dataclass
class NodeInput:
    name: str
    value: Any
    default_value: Optional[Any]


@dataclass
class NodeOverride:
    name: str
    value: Any


@dataclass
class NodeMaterializer:
    source_nodes: list[str]
    path: str
    sink: str
    data_saver: str


@dataclass
class RunMetadata:
    run_id: str
    run_dir: str
    success: bool
    # date_completed: datetime.datetime
    graph_hash: str
    modules: list[str]
    inputs: list[NodeInput]
    overrides: list[NodeOverride]
    materialized: list[NodeMaterializer]


class ExperimentTracker(lifecycle.NodeExecutionHook, lifecycle.GraphExecutionHook):
    def __init__(self, cache_path: str, run_dir: str):
        # the cache is loaded before the working directory change
        self.cache = JsonCache(cache_path=cache_path)
        self.run_id = str(uuid.uuid4())

        self.run_dir = Path(run_dir).resolve().joinpath(self.run_id)
        self.run_dir.mkdir(exist_ok=True)
        os.chdir(self.run_dir)

        self.graph_hash: str = ""
        self.modules: set[str] = set()
        self.config: dict = dict()
        self.inputs: list[NodeInput] = list()
        self.overrides: list[NodeOverride] = list()
        self.materializers: list[NodeMaterializer] = list()

    def run_before_graph_execution(
        self,
        *,
        graph: graph_types.HamiltonGraph,
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        **kwargs,
    ):
        self.overrides = [NodeOverride(name=k, value=v) for k, v in overrides.items()]
        self.graph_hash = graph_hash(graph)

        for node in graph.nodes:
            if node.tags.get("module"):
                self.modules.add(node.tags["module"])

            if node.originating_functions is None:
                self.config[node.name] = "hello"

            elif node.is_external_input:  # and (node.name not in self.config):
                self.inputs.append(
                    NodeInput(
                        name=node.name,
                        value=inputs.get(node.name),
                        default_value=_get_default_input(node),
                    )
                )

    def run_before_node_execution(self, *args, **kwargs):
        ...

    def run_after_node_execution(
        self, *, node_tags: dict, node_kwargs: dict, result: Any, **kwargs
    ):
        if node_tags.get("hamilton.data_saver") is True:
            self.materializers.append(
                NodeMaterializer(
                    source_nodes=list(node_kwargs.keys()),
                    path=str(Path(result["path"]).resolve()),
                    sink=node_tags["hamilton.data_saver.sink"],
                    data_saver=node_tags["hamilton.data_saver.classname"],
                )
            )

    def run_after_graph_execution(self, *, success: bool, **kwargs):
        run_data = dict(
            run_id=self.run_id,
            run_dir=str(self.run_dir),
            # date_completed=datetime.datetime.utcnow(),
            success=success,
            graph_hash=self.graph_hash,
            modules=list(self.modules),
            inputs=[asdict(i) for i in self.inputs],
            overrides=[asdict(o) for o in self.overrides],
            materialized=[asdict(m) for m in self.materializers],
        )

        run_json_string = json.dumps(run_data, default=json_encoder, sort_keys=True)
        self.cache.write(run_json_string, self.run_id)
