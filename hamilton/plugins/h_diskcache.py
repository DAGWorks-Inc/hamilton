import hashlib
import inspect
import logging
from typing import Any, Callable, Dict, List

from hamilton import driver, lifecycle, node

import diskcache


logger = logging.getLogger(__name__)


def _kb_to_mb(kb: int) -> float:
    return kb / (1024**2)

# TODO add this generic implementation to graph_types
# TODO maybe the hash should only depend on the function body?
def hash_callable(node_callable: Callable) -> str:
    """Create a single hash (str) from the bytecode of all sorted functions"""
    source_code = inspect.getsource(node_callable)
    return hashlib.sha256(source_code.encode()).hexdigest()


def evict_all_except(nodes_to_keep: Dict[str, node.Node], cache: diskcache.Cache) -> int:
    """Evicts all nodes and node version except those passed.
    Remaining nodes may have multiple entries for different input values 
    """
    nodes_history: Dict[str, List[str]] = cache.get(key=CacheHook.nodes_history_key)  # type: ignore
   
    new_nodes_history = dict()
    eviction_counter = 0
    for node_name, history in nodes_history.items():
        if len(history) < 1:
            continue
        
        if node_name in nodes_to_keep.keys():
            node_to_keep = nodes_to_keep[node_name]
            hash_to_keep = hash_callable(node_to_keep.callable)
            history.remove(hash_to_keep)
            new_nodes_history[node_name] = [hash_to_keep]
        
        for hash_to_evict in history:
            cache.evict(tag=f"{node_name}.{hash_to_evict}")
            eviction_counter += 1
            
    cache.set(key=CacheHook.nodes_history_key, value=new_nodes_history)
    return eviction_counter


def evict_all_except_driver(dr: driver.Driver) -> dict:
    """Wrap the utility `evict_all_except` to receive a driver.Driver object"""
    cache_hooks = [adapter for adapter in dr.adapter.adapters 
                   if isinstance(adapter, CacheHook)]
    
    if len(cache_hooks) == 0:
        raise AssertionError("0 `h_diskcache.CacheHook` defined for this Driver")
    elif len(cache_hooks) > 1:
        raise AssertionError(">1 `h_diskcache.CacheHook` defined for this Driver")
    
    cache: diskcache.Cache = cache_hooks[0].cache
    volume_before = cache.volume()
    eviction_counter = evict_all_except(nodes_to_keep=dr.graph.nodes, cache=cache)
    volume_after = cache.volume()
    volume_difference = volume_before - volume_after
    
    logger.info(f"Evicted: {_kb_to_mb(volume_difference):.2f} MB")
    logger.debug(f"Evicted {eviction_counter} entries")
    logger.debug(f"Cache size after: {_kb_to_mb(volume_after):.2f} MB")

    return dict(
        evicted_size_mb=_kb_to_mb(volume_difference),
        eviction_counter=eviction_counter,
        size_after=_kb_to_mb(volume_after),
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
        node_hash = hash_callable(node_callable)
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
