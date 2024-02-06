import hashlib
import inspect
import logging
from typing import Any, Callable, Dict, List

from hamilton import driver, lifecycle

import diskcache


logger = logging.getLogger(__name__)


def _kb_to_mb(kb: int) -> float:
    return kb / (1024**2)

# TODO add this generic implementation to graph_types
def hash_implementation(node_callable: Callable) -> str:
    """Create a single hash (str) from the bytecode of all sorted functions"""
    source_code = inspect.getsource(node_callable)
    return hashlib.sha256(source_code.encode()).hexdigest()


def evict_previous_implementations(dr: driver.Driver) -> dict:
    cache_hooks = [adapter for adapter in dr.adapter.adapters 
                   if isinstance(adapter, CacheHook)]
    
    if len(cache_hooks) == 0:
        raise AssertionError("No `h_diskcache.CacheHook` defined for this Driver")
    elif len(cache_hooks) > 1:
        raise AssertionError("More than 1 `h_diskcache.CacheHook` defined for this Driver")
    
    cache: diskcache.Cache = cache_hooks[0].cache
    
    volume_before = cache.volume()
    
    nodes_history: Dict[str, List[str]] = cache.get(key=CacheHook.nodes_history_key)  # type: ignore
    if nodes_history is None:
        return dict(
            current_volume=volume_before,
            n_nodes_evicted=0,
            memory_cleared_mb=0,
        )
    
    nodes = dr.graph.nodes.values()
    
    evicted_node_counter = 0
    for n in nodes:
        node_history = nodes_history.get(n.name, [])
        if n.callable is None:
            continue
        
        current_hash = hash_implementation(n.callable)
        node_hashes_to_evict = set(node_history).difference(set(current_hash))
        
        for hash_to_evict in node_hashes_to_evict:
            cache_tag = f"{n.name}.{hash_to_evict}"
            cache.evict(tag=cache_tag)
            evicted_node_counter += 1
    
    volume_after = cache.volume()

    return dict(
        current_volume=_kb_to_mb(volume_after),
        n_nodes_evicted=evicted_node_counter,
        memory_cleared_mb=_kb_to_mb(volume_before - volume_after),
    )


class CacheHook(
    lifecycle.NodeExecutionHook,
    lifecycle.GraphExecutionHook,
    lifecycle.NodeExecutionMethod,
):
    nodes_history_key: str = "_nodes_history"
    
    def __init__(self, cache_path: str = ".", **cache_settings):
        self.cache_path = cache_path
        self.cache = diskcache.Cache(directory=cache_path, **cache_settings)
        self.nodes_history: Dict[str, List[str]] = self.cache.get(
            key=CacheHook.nodes_history_key,default=dict()
        )  # type: ignore
        self.used_nodes_hash: Dict[str, str] = dict()

    def run_to_execute_node(
        self,
        *,
        node_name: str,
        node_callable: Any,
        node_kwargs: Dict[str, Any],
        **kwargs
    ):
        node_hash = hash_implementation(node_callable)
        self.used_nodes_hash[node_name] = node_hash
        cache_key = (node_hash, *node_kwargs.values())
        
        from_cache = self.cache.get(key=cache_key, default=None)
        if from_cache is not None:
            logger.debug(f"{node_name} {node_kwargs}: from cache")
            return from_cache
        
        logger.debug(f"{node_name} {node_kwargs}: executed")
        self.nodes_history[node_name] = self.nodes_history.get(node_name, []) + [node_hash]
        return node_callable(**node_kwargs)
        
    def run_after_node_execution(
        self, *, node_name: str, node_kwargs: dict, result: Any, **kwargs
    ):
        node_hash = self.used_nodes_hash[node_name]
        cache_key = (node_hash, *node_kwargs.values())
        cache_tag = f"{node_name}.{node_hash}"
        # only adds if key doesn't exist
        self.cache.add(key=cache_key, value=result, tag=cache_tag)

    def run_after_graph_execution(self, *args, **kwargs):
        self.cache.set(key=CacheHook.nodes_history_key, value=self.nodes_history)
        logger.info(f"Cache size: {_kb_to_mb(self.cache.volume()):.2f} MB")
        self.cache.close()
        
    def run_before_graph_execution(self, *args, **kwargs): ...

    def run_before_node_execution(self, *args, **kwargs): ...

