from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict

from hamilton import registry

registry.disable_autoload()

from hamilton import lifecycle, node
from hamilton.lifecycle import base


def _new_fn(fn, **fn_kwargs):
    """Function that runs in the thread.

    It can recursively check for Futures because we don't have to worry about
    process serialization.
    :param fn: Function to run
    :param fn_kwargs: Keyword arguments to pass to the function
    """
    for k, v in fn_kwargs.items():
        if isinstance(v, Future):
            while isinstance(v, Future):
                v = v.result()
            fn_kwargs[k] = v
    # execute the function once all the futures are resolved
    return fn(**fn_kwargs)


class FutureAdapter(base.BaseDoRemoteExecute, lifecycle.ResultBuilder):
    def __init__(self, max_workers: int = None):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        # self.executor = ProcessPoolExecutor(max_workers=max_workers)

    def do_remote_execute(
        self,
        *,
        execute_lifecycle_for_node: Callable,
        node: node.Node,
        **kwargs: Dict[str, Any],
    ) -> Any:
        """Method that is called to implement correct remote execution of hooks. This makes sure that all the pre-node and post-node hooks get executed in the remote environment which is necessary for some adapters. Node execution is called the same as before through "do_node_execute".

        :param node: Node that is being executed
        :param kwargs: Keyword arguments that are being passed into the node
        :param execute_lifecycle_for_node: Function executing lifecycle_hooks and lifecycle_methods
        """
        return self.executor.submit(_new_fn, execute_lifecycle_for_node, **kwargs)

    def build_result(self, **outputs: Any) -> Any:
        """Given a set of outputs, build the result.

        :param outputs: the outputs from the execution of the graph.
        :return: the result of the execution of the graph.
        """
        for k, v in outputs.items():
            if isinstance(v, Future):
                outputs[k] = v.result()
        return outputs
