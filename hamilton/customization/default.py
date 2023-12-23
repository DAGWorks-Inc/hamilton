"""A selection of default lifeycle hooks/methods that come with Hamilton. These carry no additional requirements"""
import pprint
import time
from typing import Any, Callable, Dict, Optional

from hamilton.customization import NodeExecutionHook

NodeFilter = Callable[
    [str, Dict[str, Any]], bool
]  # filter fucntion for nodes, mapping node name and node tags to a boolean


class PrintLnHook(NodeExecutionHook):
    """Basic hook to print out information before/after node execution."""

    NODE_TIME_STATE = "node_time"

    @staticmethod
    def _format_time_delta(delta: float) -> str:
        """Formats a time delta into a human-readable string."""
        # Determine the appropriate unit and format
        if delta < 0.001:  # Less than 1 millisecond
            return f"{delta * 1e6:.3g}μs"
        elif delta < 1:  # Less than 1 second
            return f"{delta * 1e3:.3g}ms"
        return f"{delta:.3g}s"

    @staticmethod
    def _validate_verbosity(verbosity: int):
        """Validates that the verbosity is one of [1, 2]"""
        if verbosity not in [1, 2]:
            raise ValueError(f"Verbosity must be one of [1, 2], got {verbosity}")

    @staticmethod
    def _format_node_name(node_name: str, task_id: Optional[str]) -> str:
        """Formats a node name and task id into a unique node name."""
        if task_id is not None:
            return f"{task_id}:{node_name}"
        return node_name

    def __init__(
        self,
        verbosity: int = 1,
        print_fn: Callable[[str], None] = print,
        node_filter: Optional[NodeFilter] = None,
    ):
        """Prints out information before/after node execution.

        :param verbosity: The verbosity level to print out at.
            `verbosity=1` Print out just the node name and time it took to execute
            `verbosity=2`. Print out inputs of the node + results on execute
        :param print_fn: A function that takes a string and prints it out -- defaults to print. Pass in a logger function, etc... if you so choose.
        :param node_filter: A function that takes a node name and a node tags dict and returns a boolean. If the boolean is True, the node will be printed out.
            If False, it will not be printed out.
        """
        PrintLnHook._validate_verbosity(verbosity)
        self.verbosity = verbosity
        self.print_fn = print_fn
        self.timer_dict = {}  # quick dict to track the time it took to execute a node
        if node_filter is None:
            node_filter = lambda node_name, node_tags: True  # noqa E731
        self.node_filter = node_filter

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        task_id: Optional[str],
        **future_kwargs: Any,
    ):
        """Runs before a node executes. Prints out the node name and inputs if verbosity is 2.

        :param node_name: Name of the node
        :param node_tags: Tags of the node
        :param node_kwargs: Keyword arguments of the node
        :param task_id: ID of the task that the node is in, if any
        :param future_kwargs: Additional keyword arguments that may be passed to the hook yet are ignored for now
        """
        if not self.node_filter(node_name, node_tags):
            return
        node_unique_id = self._format_node_name(node_name, task_id)
        self.timer_dict[node_unique_id] = time.time()
        message = f"Executing node: {node_unique_id}."
        if self.verbosity == 2:
            message += f" Inputs: \n{pprint.pformat(node_kwargs)}"
        self.print_fn(message)

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        result: Any,
        error: Optional[Exception],
        success: bool,
        task_id: Optional[str],
        **future_kwargs: Any,
    ):
        """Runs after a node executes. Prints out the node name and time it took, the output if verbosity is 1.

        :param node_name: Name of the node
        :param node_tags: Tags of the node
        :param node_kwargs: Keyword arguments passed to the node
        :param result: Result of the node
        :param error: Error of the node
        :param success: Whether the node was successful or not
        :param task_id: ID of the task that the node is in, if any
        :param future_kwargs: Additional keyword arguments that may be passed to the hook yet are ignored for now
        """
        if not self.node_filter(node_name, node_tags):
            return
        node_unique_id = self._format_node_name(node_name, task_id)
        time_delta = time.time() - self.timer_dict[node_unique_id]
        time_delta_formatted = self._format_time_delta(time_delta)
        message = f"Finished executing node: {node_unique_id} in {time_delta_formatted}. Status: {'Success' if success else 'Failure'}."
        if self.verbosity == 2:
            if success:
                message += f" Result: \n{pprint.pformat(result)}\n"
            else:
                message += f" Error: \n{pprint.pformat(error)}"
        self.print_fn(message)
        del self.timer_dict[node_name]
