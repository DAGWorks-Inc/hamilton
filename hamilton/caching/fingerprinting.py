import base64
import functools
import hashlib
import json
import logging
import pathlib
from collections.abc import Callable, Mapping, Sequence, Set
from typing import Any, Dict, Optional

from hamilton.experimental import h_databackends
from hamilton.lifecycle import GraphExecutionHook, NodeExecutionHook

logger = logging.getLogger(__name__)


def _compact_hash(digest: bytes) -> str:
    return base64.urlsafe_b64encode(digest).decode()


# TODO handle hashing None
@functools.singledispatch
def hash_value(obj, depth=0, *args, **kwargs) -> str:
    """Fingerprinting strategy that computes a hash of the
    full Python object.

    The default case hashes the `__dict__` attribute of the
    object (recursive).
    """
    MAX_DEPTH = 3
    if hasattr(obj, "__dict__") and depth < MAX_DEPTH:
        depth += 1
        return hash_value(obj.__dict__, depth)

    hash_object = hashlib.md5("<unhashable>".encode())
    return _compact_hash(hash_object.digest())


@hash_value.register(str)
@hash_value.register(int)
@hash_value.register(float)
@hash_value.register(bool)
@hash_value.register(bytes)
def hash_primitive(obj, *args, **kwargs) -> str:
    """Convert the primitive to a string and hash it"""
    hash_object = hashlib.md5(str(obj).encode())
    return _compact_hash(hash_object.digest())


@hash_value.register(Sequence)
def hash_sequence(obj, *, sort: bool = False, **kwargs) -> str:
    """Hash each object of the sequence.

    Orders matters for the hash since orders matters in a sequence.
    """
    hash_object = hashlib.sha224()
    for elem in obj:
        hash_object.update(hash_value(elem).encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(Mapping)
def hash_mapping(obj, *, sort: bool = False, **kwargs) -> str:
    """Hash each key then its value.

    The mapping is always sorted first because order shouldn't matter
    in a mapping.

    NOTE this may clash with Python dictionary ordering since >=3.7
    """
    if sort:
        obj = dict(sorted(obj.items()))

    hash_object = hashlib.sha224()
    for key, value in obj.items():
        hash_object.update(hash_value(key).encode())
        hash_object.update(hash_value(value).encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(Set)
def hash_set(obj, *args, **kwargs) -> str:
    """Hash each element of the set, then sort hashes, and
    create a hash of hashes.

    For the same objects in the set, the hashes will be the
    same.
    """
    hashes = [hash_value(elem) for elem in obj]
    sorted_hashes = sorted(hashes)

    hash_object = hashlib.sha224()
    for hash in sorted_hashes:
        hash_object.update(hash.encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(h_databackends.AbstractPandasDataFrame)
def hash_pandas_dataframe(obj, *args, **kwargs) -> str:
    """Convert a pandas dataframe to a dictionary of {index: row_hash}
    then hash it.

    Given the hashing for mappings, the physical ordering or rows doesn't matter.
    For example, if the index is a date, the hash will represent the {date: row_hash},
    and won't preserve how dates were ordered in the DataFrame.
    """
    from pandas.util import hash_pandas_object

    hash_per_row = hash_pandas_object(obj)
    return hash_value(hash_per_row.to_dict())


class FingerprintingAdapter(GraphExecutionHook, NodeExecutionHook):
    def __init__(self, path: Optional[str] = None, strategy: Optional[Callable] = None):
        """Fingerprint node results. This is primarily an interval tool for developing
        and debugging caching features.

        If path is specified, output a {node_name: fingerprint} to ./fingerprints/{run_id}.json
        Strategy allows to pass different callables to fingerprint values. Works well with
        `@functools.single_dispatch`. See `hash_value()` for reference
        """
        self.path = path
        self.fingerprint = strategy if strategy else hash_value
        self.run_data_hashes = {}

    def run_before_graph_execution(
        self, *, run_id: str, inputs: Dict[str, Any], overrides: Dict[str, Any], **kwargs: Any
    ):
        """Get the fingerprint of inputs and overrides before execution.

        It's the ideal place to fingerprint these values since they never pass through the hooks
        `run_to_execute_node()` or `run_after_node_execution()`
        """
        self.run_id = run_id

        if inputs:
            for node_name, value in inputs.items():
                self.run_data_hashes[node_name] = self.fingerprint(value)

        if overrides:
            for node_name, value in overrides.items():
                self.run_data_hashes[node_name] = self.fingerprint(value)

    def run_after_node_execution(self, *, node_name: str, result: Any, **kwargs):
        """Get the fingerprint of the most recent node result"""
        # values passed as inputs or overrides will already have known hashes
        data_hash = self.run_data_hashes.get(node_name)
        if data_hash is None:
            self.run_data_hashes[node_name] = self.fingerprint(result)

    def run_before_node_execution(self, *args, **kwargs):
        """Placeholder required to subclass `NodeExecutionHook`"""

    def run_after_graph_execution(self, *args, **kwargs):
        """If path is specified, output a {node_name: fingerprint} to ./fingerprints/{run_id}.json"""
        if self.path:
            file_path = pathlib.Path(self.path, "fingerprints", f"{self.run_id}.json")
            file_path.parent.mkdir(exist_ok=True)
            file_path.write_text(json.dumps(self.run_data_hashes))
