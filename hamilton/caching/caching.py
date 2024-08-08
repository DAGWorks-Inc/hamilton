import base64
import logging
import zlib
from typing import Any, Dict, Optional

from hamilton import graph_types
from hamilton.caching import fingerprinting
from hamilton.caching.repository import ShelveRepository, dbmRepository
from hamilton.lifecycle import GraphExecutionHook, NodeExecutionHook, NodeExecutionMethod

logger = logging.getLogger(__name__)


def _compress_string(string: str) -> str:
    return base64.b64encode(zlib.compress(string.encode(), level=3)).decode()


def _decompress_string(string: str) -> str:
    return zlib.decompress(base64.b64decode(string.encode())).decode()


def _encode_str_dict(d: dict) -> str:
    interleaved_tuple = tuple(item for pair in sorted(d.items()) for item in pair)
    return " ".join(interleaved_tuple)


def _decode_str_dict(s: str) -> dict:
    interleaved_tuple = tuple(s.split(" "))
    d = {}
    for i in range(0, len(interleaved_tuple), 2):
        d[interleaved_tuple[i]] = interleaved_tuple[i + 1]
    return d


def encode_dict(hash_map: Dict[str, str]) -> str:
    """Store input fingerprints as single string using a revertable encoding.

    For example:
        the input: {"node_a": "version_1", "node_b": "version_2"}
        will be encoded as: 'node_a version_1 node_b version_2'
        and then compress to: 'eF7Ly09JjU9UKEstKs7Mz4s3VMgDCSTBBYwA1BsMWw=='

    NOTE. the compressed string is longer than the original string here
    because `version_1` is much shorter than the real version SHA256 hashes.
    """
    interleaved_string = _encode_str_dict(hash_map)
    return _compress_string(interleaved_string)


def decode_dict(encoded_dict: str):
    """Convert encoded input fingerprints back to a dictionary of {node_name: version}
    Does the opposite of `encode_inputs()`

    For example:
        the compressed encoded string: 'eF7Ly09JjU9UKEstKs7Mz4s3VMgDCSTBBYwA1BsMWw=='
        is decompressed to: 'node_a version_1 node_b version_2'
        then converted back to a dictionary: {"node_a": "version_1", "node_b": "version_2"}
    """
    interleaved_string = _decompress_string(encoded_dict)
    return _decode_str_dict(interleaved_string)


"""
Design decisions:
- Decouple adapter, data fingerprint store, result store
- write {data_fingerprint: result} and {inputs_fingerprint: output_fingerprint} a
  after each node execution vs. doing it at the end of graph execution
- by passing as inputs `current_data_fingerprints` you're essentially doing "overrides" from disk
"""


# TODO change temporary name
class SmartCache(NodeExecutionHook, NodeExecutionMethod, GraphExecutionHook):
    def __init__(
        self,
        fingerprint_store=dbmRepository("fingerprints"),
        result_store=ShelveRepository("results"),
        fingerprints: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.fingerprint_store = fingerprint_store
        self.result_store = result_store
        self.code_versions: Dict[str, Any] = dict()  # {node_name: code_version}
        self.run_fingerprints = fingerprints if fingerprints else {}  # {node_name: fingerprint}

    def run_before_graph_execution(
        self,
        *,
        graph: graph_types.HamiltonGraph,
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        **kwargs: Any,
    ):
        """Get code versions for all, and data version for top-level inputs and overrides
        Open the cache
        """
        # both should be reset before graph execution
        # NOTE that code_version shouldn't change for the lifetime of a driver
        self.code_versions = {n.name: n.version for n in graph.nodes}
        self.run_fingerprints = {}
        # TODO handle locking vs. non-locking stores
        self.fingerprint_store.open()
        self.result_store.open()

        # need to hash top-level inputs and store in run `data_versions` to set the
        # base case of the recursive `create_input_keys()`
        if inputs:
            for node_name, value in inputs.items():
                fingerprint = fingerprinting.hash_value(value)
                self.run_fingerprints[node_name] = fingerprint
                # for top-level inputs, the code version shouldn't be considered in the key
                self.fingerprint_store.set(key="<inputs>", value=fingerprint, node_name=node_name)
                self.result_store.set(key=fingerprint, value=value, node_name=node_name)

        if overrides:
            for node_name, value in overrides.items():
                fingerprint = fingerprinting.hash_value(value)
                self.run_fingerprints[node_name] = fingerprint
                # for overrides, we don't want to store a `(code, inputs_key): fingerprint`
                # because the node didn't actually ran.
                # We can still store the `fingerprint: value` in case we hit that fingerprint
                # on subsequent runs
                self.result_store.set(key=fingerprint, value=value, node_name=node_name)

    def run_after_node_execution(
        self, *, node_name: str, node_kwargs: Dict[str, Any], result: Any, **kwargs
    ):
        """Try to read data version from memory or from cache else compute result version.
        Then, store data version in memory (for this run) and data version cache (for next run),
        and store result in result cache (for retrieval in next run)
        """
        # read data version from current run or previously retrieved
        fingerprint = self.run_fingerprints.get(node_name)
        # read data version of previous runs
        if fingerprint is None:
            # create the key from (code version, inputs data version)
            dependencies_fingerpints = {
                name: self.run_fingerprints[name] for name in node_kwargs.keys()
            }
            dependencies_encoded = encode_dict(dependencies_fingerpints)
            output_key = encode_dict({self.code_versions[node_name]: dependencies_encoded})
            fingerprint = self.fingerprint_store.get(output_key, node_name=node_name)

        # compute data version when cache misses
        if fingerprint is None:
            logger.debug(f"{node_name}: storing fingerprint")
            fingerprint = fingerprinting.hash_value(result)
            self.fingerprint_store.set(key=output_key, value=fingerprint, node_name=node_name)
            logger.debug(f"{node_name}: storing result")
            self.result_store.set(key=fingerprint, value=result, node_name=node_name)

        self.run_fingerprints[node_name] = fingerprint
        # NOTE avoid doing unecessary writes for results already stored
        # the validation should be implemented in `.set_result()` since it's an I/O concern

    # NOTE instead of passing values through, node_kwargs could pass pointers to files
    # this brings us closer to macro orchestration
    def run_to_execute_node(
        self, *, node_name: str, node_callable: Any, node_kwargs: Dict[str, Any], **kwargs
    ):
        """Create hash key then use cached value if exist"""
        fingerprint = self.run_fingerprints.get(node_name)
        # read data version from cache for previous runs
        if fingerprint is None:
            dependencies_fingerprints = {
                name: self.run_fingerprints[name] for name in node_kwargs.keys()
            }
            dependencies_encoded = encode_dict(dependencies_fingerprints)
            output_key = encode_dict({self.code_versions[node_name]: dependencies_encoded})
            fingerprint = self.fingerprint_store.get(output_key, node_name=node_name)

        if fingerprint is None:
            logger.debug(f"{node_name}: executing")
            return node_callable(**node_kwargs)

        result_from_cache = self.result_store.get(fingerprint, node_name=node_name)
        if result_from_cache is None:
            # NOTE Could raise an exception here because data_version cache expected to find a stored result
            return node_callable(**node_kwargs)

        logger.debug(f"{node_name}: result from cache")
        return result_from_cache

    def run_after_graph_execution(self, *args, **kwargs):
        """Close the store"""
        self.fingerprint_store.close()
        self.result_store.close()

    def run_before_node_execution(self, *args, **kwargs):
        """Placeholder required to subclass `NodeExecutionHook`"""
