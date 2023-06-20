import json
import logging
import os
from typing import Any, Callable, Optional

import pandas as pd
from hamilton.base import SimplePythonGraphAdapter
from hamilton.node import Node

logger = logging.getLogger(__name__)


def write_feather(data: pd.DataFrame, filepath: str) -> None:
    """Writes a data frame to a feather file."""
    data.to_feather(filepath)


def read_feather(filepath: str) -> pd.DataFrame:
    """Reads a data frame from a feather file."""
    return pd.read_feather(filepath)


def write_parquet(data: pd.DataFrame, filepath: str) -> None:
    """Writes a data frame to a parquet file."""
    data.to_parquet(filepath)


def read_parquet(filepath: str) -> pd.DataFrame:
    """Reads a data frame from a parquet file."""
    return pd.read_parquet(filepath)


def write_json(data: dict, filepath: str) -> None:
    """Writes a dictionary to a JSON file."""
    with open(filepath, "w", encoding="utf8") as file:
        json.dump(data, file)


def read_json(filepath: str) -> dict:
    """Reads a dictionary from a JSON file."""
    with open(filepath, "r", encoding="utf8") as file:
        return json.load(file)


class CachingAdapter(SimplePythonGraphAdapter):
    """Caching adapter.

    Any node with tag "cache" will be cached (or loaded from cache) in the format defined by the
    tag's value. There are a handful of formats supported, and other formats' readers and writers
    can be provided to the constructor.

    Values are loaded from cache if the node's file exists, unless one of these is true:
     * node is explicitly forced to be computed with a constructor argument,
     * any of its (potentially transitive) dependencies that are configured to be cached
       was nevertheless computed (either forced or missing cached file).
    """

    def __init__(
        self,
        cache_path: str,
        *args,
        force_compute: Optional[set[str]] = None,
        writers: Optional[dict[str, Callable[[Any, str], None]]] = None,
        readers: Optional[dict[str, Callable[[str], Any]]] = None,
        **kwargs,
    ):
        """Constructs the adapter.

        Parameters
        ----------
        cache_path : str
            Path to the directory where cached files are stored.
        force_compute : optional, set
            Set of nodes that should be forced to compute even if cache exists.
        writers : optional, dict
            A dictionary of writers for custom formats.
        readers : optional, dict
            A dictionary of readers for custom formats.
        """

        super().__init__(*args, **kwargs)
        self.cache_path = cache_path
        self.force_compute = force_compute if force_compute is not None else {}
        self.computed_nodes = set()

        self.writers = writers or {}
        self.readers = readers or {}

        self._init_default_readers_writers()

    def _init_default_readers_writers(self):
        if "json" not in self.writers:
            self.writers["json"] = write_json
        if "json" not in self.readers:
            self.readers["json"] = read_json

        if "feather" not in self.writers:
            self.writers["feather"] = write_feather
        if "feather" not in self.readers:
            self.readers["feather"] = read_feather

        if "parquet" not in self.writers:
            self.writers["parquet"] = write_parquet
        if "parquet" not in self.readers:
            self.readers["parquet"] = read_parquet

    def _check_format(self, fmt):
        if fmt not in self.writers:
            raise ValueError(f"invalid cache format: {fmt}")

    def _write_cache(self, data: Any, fmt: str, filepath: str) -> None:
        self._check_format(fmt)
        self.writers[fmt](data, filepath)

    def _read_cache(self, fmt: str, filepath: str) -> None:
        self._check_format(fmt)
        return self.readers[fmt](filepath)

    def execute_node(self, node: Node, kwargs: dict[str, Any]) -> Any:
        cache_format = node.tags.get("cache")
        implicitly_forced = any(dep.name in self.computed_nodes for dep in node.dependencies)
        if cache_format is not None:
            filepath = f"{self.cache_path}/{node.name}.{cache_format}"
            explicitly_forced = node.name in self.force_compute
            if explicitly_forced or implicitly_forced or not os.path.exists(filepath):
                result = node.callable(**kwargs)
                self._write_cache(result, cache_format, filepath)
                self.computed_nodes.add(node.name)
                return result
            return self._read_cache(cache_format, filepath)

        if implicitly_forced:
            # For purposes of caching, we only mark it as computed if any cached input was computed.
            # Otherwise, dependants would always be recomputed if they have a non-cached dependency.
            self.computed_nodes.add(node.name)
        return node.callable(**kwargs)
