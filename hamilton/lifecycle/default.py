"""A selection of default lifeycle hooks/methods that come with Hamilton. These carry no additional requirements"""

import logging
import pdb
import pprint
import random
import time
from typing import Any, Callable, Dict, List, Optional, Union

from hamilton import htypes
from hamilton.lifecycle import NodeExecutionHook
from hamilton.lifecycle.api import NodeExecutionMethod

logger = logging.getLogger(__name__)

NodeFilter = Union[
    Callable[[str], Dict[str, Any]],
    bool,  # filter function for nodes, mapping node name to a boolean
    List[str],  # list of node names to run
    str,  # node name to run
    None,  # run all nodes
]  # filter function for nodes, mapping node name and node tags to a boolean


def should_run_node(node_name: str, node_tags: Dict[str, Any], node_filter: NodeFilter) -> bool:
    if node_filter is None:
        return True
    if isinstance(node_filter, str):
        return node_name == node_filter
    if isinstance(node_filter, list):
        return node_name in node_filter
    if callable(node_filter):
        return node_filter(node_name, node_tags)
    raise ValueError(f"Invalid node filter: {node_filter}")


class PrintLn(NodeExecutionHook):
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
        node_filter: NodeFilter = None,
    ):
        """Prints out information before/after node execution.

        :param verbosity: The verbosity level to print out at.
            `verbosity=1` Print out just the node name and time it took to execute
            `verbosity=2`. Print out inputs of the node + results on execute
        :param print_fn: A function that takes a string and prints it out -- defaults to print. Pass in a logger function, etc... if you so choose.
        :param node_filter: A function that takes a node name and a node tags dict and returns a boolean. If the boolean is True, the node will be printed out.
            If False, it will not be printed out.
        """
        PrintLn._validate_verbosity(verbosity)
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
        if not should_run_node(node_name, node_tags, self.node_filter):
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
        if not should_run_node(node_name, node_tags, self.node_filter):
            return
        node_unique_id = self._format_node_name(node_name, task_id)
        time_delta = time.time() - self.timer_dict[node_unique_id]
        time_delta_formatted = self._format_time_delta(time_delta)
        message = f"Finished debugging node: {node_unique_id} in {time_delta_formatted}. Status: {'Success' if success else 'Failure'}."
        if self.verbosity == 2:
            if success:
                message += f" Result: \n{pprint.pformat(result)}\n"
            else:
                message += f" Error: \n{pprint.pformat(error)}"
        self.print_fn(message)
        del self.timer_dict[node_name]


class PDBDebugger(NodeExecutionHook, NodeExecutionMethod):
    """Class to inject a PDB debugger into a node execution. This is still somewhat experimental as it is a debugging utility.
    We reserve the right to change the API and the implementation of this class in the future.
    """

    CONTEXT = dict()

    def __init__(
        self,
        node_filter: NodeFilter,
        before: bool = False,
        during: bool = False,
        after: bool = False,
    ):
        """Creates a PDB debugger. This has three possible modes:
            1. Before -- places you in a function with (a) node information, and (b) inputs
            2. During -- runs the node with pdb.run. Note this may not always work or give what you expect as
                node functions are often wrapped in multiple levels of input modifications/whatnot. That said, it should give you something.
                Also note that this is not (currently) compatible with graph adapters.
            3. After -- places you in a function with (a) node information, (b) inputs, and (c) results


        :param node_filter: A function that takes a node name and a node tags dict and returns a boolean. If the boolean is True, the node will be printed out.
        :param before: Whether to place you in a PDB debugger before a node executes
        :param during: Whether to place you in a PDB debugger during a node's execution
        :param after: Whether to place you in a PDB debugger after a node executes
        """
        self.node_filter = node_filter
        self.run_before = before
        self.run_during = during
        self.run_after = after

    def run_to_execute_node(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_callable: Any,
        node_kwargs: Dict[str, Any],
        task_id: Optional[str],
        **future_kwargs: Any,
    ) -> Any:
        """Executes the node with a PDB debugger. This modifies the global PDBDebugger.CONTEXT variable to contain information about the node,
            so you can access it while debugging.


        :param node_name: Name of the node
        :param node_tags: Tags of the node
        :param node_callable: Callable function of the node
        :param node_kwargs: Keyword arguments passed to the node
        :param task_id: ID of the task that the node is in, if any
        :param future_kwargs: Additional keyword arguments that may be passed to the hook yet are ignored for now
        :return: Result of the node
        """
        if not should_run_node(node_name, node_tags, self.node_filter) or not self.run_during:
            return node_callable(**node_kwargs)
        PDBDebugger.CONTEXT = {
            "node_name": node_name,
            "node_tags": node_tags,
            "node_callable": node_callable,
            "node_kwargs": node_kwargs,
            "task_id": task_id,
            "future_kwargs": future_kwargs,
        }
        logging.warning(
            (
                f"Placing you in a PDB debugger for node {node_name}."
                "\nYou can access additional node information via PDBDebugger.CONTEXT. Data is:"
                f"\n - node_name: {PDBDebugger._truncate_repr(node_name)}"
                f"\n - node_tags: {PDBDebugger._truncate_repr(node_tags)}"
                f"\n - node_callable: {PDBDebugger._truncate_repr(node_callable)}"
                f"\n - node_kwargs: {PDBDebugger._truncate_repr(', '.join(list(node_kwargs.keys())))}"
                f"\n - task_id: {PDBDebugger._truncate_repr(task_id)}"
                f"\n - future_kwargs: {PDBDebugger._truncate_repr(future_kwargs)}"
            )
        )
        out = pdb.runcall(node_callable, **node_kwargs)
        logging.info(f"Finished executing node {node_name}.")
        return out

    @staticmethod
    def _truncate_repr(obj: Any, num_chars: int = 80) -> str:
        """Truncates the repr of an object to 100 characters."""
        if isinstance(obj, str):
            return obj[:num_chars]
        return repr(obj)[:num_chars]

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        task_id: Optional[str],
        **future_kwargs: Any,
    ):
        """Executes before a node executes. Does nothing, just runs pdb.set_trace()

        :param node_name: Name of the node
        :param node_tags: Tags of the node
        :param node_kwargs: Keyword arguments passed to the node
        :param node_return_type: Return type of the node
        :param task_id: ID of the task that the node is in, if any
        :param future_kwargs: Additional keyword arguments that may be passed to the hook yet are ignored for now
        :return: Result of the node
        """
        if should_run_node(node_name, node_tags, self.node_filter) and self.run_before:
            logging.warning(
                (
                    f"Placing you in a PDB debugger prior to execution of node: {node_name}."
                    "\nYou can access additional node information via the following variables:"
                    f"\n - node_name: {PDBDebugger._truncate_repr(node_name)}"
                    f"\n - node_tags: {PDBDebugger._truncate_repr(node_tags)}"
                    f"\n - node_kwargs: {PDBDebugger._truncate_repr(', '.join(list(node_kwargs.keys())))}"
                    f"\n - node_return_type: {PDBDebugger._truncate_repr(node_return_type)}"
                    f"\n - task_id: {PDBDebugger._truncate_repr(task_id)}"
                )
            )
            pdb.set_trace()

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
        **future_kwargs: Any,
    ):
        """Executes after a node, whether or not it was successful. Does nothing, just runs pdb.set_trace().

        :param node_name: Name of the node
        :param node_tags:  Tags of the node
        :param node_kwargs:  Keyword arguments passed to the node
        :param node_return_type:  Return type of the node
        :param result: Result of the node, None if there was an error
        :param error: Error of the node, None if there was no error
        :param success:  Whether the node ran successful or not
        :param task_id: Task ID of the node, if any
        :param future_kwargs: Additional keyword arguments that may be passed to the hook yet are ignored for now
        """
        if should_run_node(node_name, node_tags, self.node_filter) and self.run_after:
            logging.warning(
                (
                    f"Placing you in a PDB debugger post execution of node: {node_name}."
                    "\nYou can access additional node information via the following variables:"
                    f"\n - node_name: {PDBDebugger._truncate_repr(node_name)}"
                    f"\n - node_tags: {PDBDebugger._truncate_repr(node_tags)}"
                    f"\n - node_kwargs: {PDBDebugger._truncate_repr(', '.join(list(node_kwargs.keys())))}"
                    f"\n - node_return_type: {PDBDebugger._truncate_repr(node_return_type)}"
                    f"\n - result: {PDBDebugger._truncate_repr(result)}"
                    f"\n - error: {PDBDebugger._truncate_repr(error)}"
                    f"\n - success: {PDBDebugger._truncate_repr(success)}"
                    f"\n - task_id: {PDBDebugger._truncate_repr(task_id)}"
                )
            )
            pdb.set_trace()


def wait_random(mean: float, stddev: float):
    sleep_time = random.gauss(mu=mean, sigma=stddev)
    if sleep_time < 0:
        sleep_time = 0
    time.sleep(sleep_time)


class SlowDownYouMoveTooFast(NodeExecutionHook):
    """This hook makes your DAG run slower. Just pass in a negative value for the sleep time to make it go faster...
    In all seriousness though, there is absolutely no (good) reason to use this hook. Its dumb, and just for testing.
    """

    def __init__(self, sleep_time_mean: float, sleep_time_std: float):
        """In all seriousness, don't use this

        :param sleep_time_mean: Mean of sleep time
        :param sleep_time_std: Stddev of sleep time
        """
        self.sleep_time_mean = sleep_time_mean
        self.sleep_time_std = sleep_time_std

    def run_before_node_execution(self, **future_kwargs: Any):
        """Waits for a fixed set of time before node execution"""
        wait_random(self.sleep_time_mean, self.sleep_time_std)

    def run_after_node_execution(self, **future_kwargs: Any):
        """Does nothing"""
        pass


class FunctionInputOutputTypeChecker(NodeExecutionHook):
    """This lifecycle hook checks the input and output types of a function.

    It is a simple, but very strict type check against the declared type with what was actually received.
    E.g. if you don't want to check the types of a dictionary, don't annotate it with a type.
    """

    def __init__(self, check_input: bool = True, check_output: bool = True):
        """Constructor.

        :param check_input: check inputs to all functions
        :param check_output: check outputs to all functions
        """
        self.check_input = check_input
        self.check_output = check_output

    def run_before_node_execution(
        self,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        task_id: Optional[str],
        run_id: str,
        node_input_types: Dict[str, Any],
        **future_kwargs: Any,
    ):
        """Checks that the result type matches the expected node return type."""
        if self.check_input:
            for input_name, input_value in node_kwargs.items():
                if not htypes.check_instance(input_value, node_input_types[input_name]):
                    raise TypeError(
                        f"Node {node_name} received an input of type {type(input_value)} for {input_name}, expected {node_input_types[input_name]}"
                    )

    def run_after_node_execution(
        self,
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
        """Checks that the result type matches the expected node return type."""
        if self.check_output:
            # Replace the isinstance check in your code with check_instance
            if not htypes.check_instance(result, node_return_type):
                raise TypeError(
                    f"Node {node_name} returned a result of type {type(result)}, expected {node_return_type}"
                )
