import abc
import functools
import logging
import sys
import time

# required if we want to run this code stand alone.
import typing
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from types import ModuleType
from typing import Any, Callable, Collection, Dict, List, Optional, Set, Tuple, Union

import pandas as pd

from hamilton import common
from hamilton.execution import executors, graph_functions, grouping, state
from hamilton.io import materialization

SLACK_ERROR_MESSAGE = (
    "-------------------------------------------------------------------\n"
    "Oh no an error! Need help with Hamilton?\n"
    "Join our slack and ask for help! https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg\n"
    "-------------------------------------------------------------------\n"
)

if __name__ == "__main__":
    import base
    import graph
    import node
    import telemetry
else:
    from . import base, graph, node, telemetry

logger = logging.getLogger(__name__)


def capture_function_usage(call_fn: Callable) -> Callable:
    """Decorator to wrap some driver functions for telemetry capture.

    We want to use this for non-constructor and non-execute functions.
    We don't capture information about the arguments at this stage,
    just the function name.

    :param call_fn: the Driver function to capture.
    :return: wrapped function.
    """

    @functools.wraps(call_fn)
    def wrapped_fn(*args, **kwargs):
        try:
            return call_fn(*args, **kwargs)
        finally:
            if telemetry.is_telemetry_enabled():
                try:
                    function_name = call_fn.__name__
                    event_json = telemetry.create_driver_function_invocation_event(function_name)
                    telemetry.send_event_json(event_json)
                except Exception as e:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.error(
                            f"Failed to send telemetry for function usage. Encountered:{e}\n"
                        )

    return wrapped_fn


@dataclass
class Variable:
    """External facing API for hamilton. Having this as a dataclass allows us
    to hide the internals of the system but expose what the user might need.
    Furthermore, we can always add attributes and maintain backwards compatibility."""

    name: str
    type: typing.Type
    tags: Dict[str, str] = field(default_factory=dict)
    is_external_input: bool = field(default=False)
    originating_functions: Optional[Tuple[Callable, ...]] = None

    @staticmethod
    def from_node(n: node.Node) -> "Variable":
        """Creates a Variable from a Node.

        :param n: Node to create the Variable from.
        :return: Variable created from the Node.
        """
        return Variable(
            name=n.name,
            type=n.type,
            tags=n.tags,
            is_external_input=n.user_defined,
            originating_functions=n.originating_functions,
        )


class InvalidExecutorException(Exception):
    """Raised when the executor is invalid for the given graph."""

    pass


class GraphExecutor(abc.ABC):
    """Interface for the graph executor. This runs a function graph,
    given a function graph, inputs, and overrides."""

    @abc.abstractmethod
    def execute(
        self,
        fg: graph.FunctionGraph,
        final_vars: List[Union[str, Callable, Variable]],
        overrides: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Executes a graph in a blocking function.

        :param fg: Graph to execute
        :param final_vars: Variables we want
        :param overrides: Overrides --- these short-circuit computation
        :param inputs: Inputs to the Graph.
        :return: The output of the final variables, in dictionary form.
        """

    @abc.abstractmethod
    def validate(self, nodes_to_execute: List[node.Node]):
        """Validates whether the executor can execute the given graph.
        Some executors allow API constructs that others do not support
        (such as Parallelizable[]/Collect[])

        :param fg: Graph to execute
        :return: Whether or not the executor can execute the graph.
        """
        pass


class DefaultGraphExecutor(GraphExecutor):
    def validate(self, nodes_to_execute: List[node.Node]):
        """The default graph executor cannot handle parallelizable[]/collect[] nodes.

        :param nodes_to_execute:
        :raises InvalidExecutorException: if the graph contains parallelizable[]/collect[] nodes.
        """
        for node_ in nodes_to_execute:
            if node_.node_role in (node.NodeType.EXPAND, node.NodeType.COLLECT):
                raise InvalidExecutorException(
                    f"Default graph executor cannot handle parallelizable[]/collect[] nodes. "
                    f"Node {node_.name} defined by functions: "
                    f"{[fn.__qualname__ for fn in node_.originating_functions]}"
                )

    def execute(
        self,
        fg: graph.FunctionGraph,
        final_vars: List[str],
        overrides: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Basic executor for a function graph. Does no task-based execution, just does a DFS
        and executes the graph in order, in memory."""
        memoized_computation = dict()  # memoized storage
        nodes = [fg.nodes[node_name] for node_name in final_vars]
        fg.execute(nodes, memoized_computation, overrides, inputs)
        outputs = {
            final_var: memoized_computation[final_var] for final_var in final_vars
        }  # only want request variables in df.
        del memoized_computation  # trying to cleanup some memory
        return outputs


class TaskBasedGraphExecutor(GraphExecutor):
    def validate(self, nodes_to_execute: List[node.Node]):
        """Currently this can run every valid graph"""
        pass

    def __init__(
        self,
        execution_manager: executors.ExecutionManager,
        grouping_strategy: grouping.GroupingStrategy,
        adapter: base.HamiltonGraphAdapter,
    ):
        """Executor for task-based execution. This enables grouping of nodes into tasks, as
        well as parallel execution/dynamic spawning of nodes.

        :param execution_manager: Utility to assign task executors to node groups
        :param grouping_strategy: Utility to group nodes into tasks
        :param result_builder: Utility to build the final result"""

        self.execution_manager = execution_manager
        self.grouping_strategy = grouping_strategy
        self.adapter = adapter

    def execute(
        self,
        fg: graph.FunctionGraph,
        final_vars: List[str],
        overrides: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Executes a graph, task by task. This blocks until completion.

        This does the following:
        1. Groups the nodes into tasks
        2. Creates an execution state and a results cache
        3. Runs it to completion, populating the results cache
        4. Returning the results from the results cache
        """
        inputs = graph_functions.combine_config_and_inputs(fg.config, inputs)
        (
            transform_nodes_required_for_execution,
            user_defined_nodes_required_for_execution,
        ) = fg.get_upstream_nodes(final_vars, runtime_inputs=inputs, runtime_overrides=overrides)

        all_nodes_required_for_execution = list(
            set(transform_nodes_required_for_execution).union(
                user_defined_nodes_required_for_execution
            )
        )
        grouped_nodes = self.grouping_strategy.group_nodes(
            all_nodes_required_for_execution
        )  # pure function transform
        # Instantiate a result cache so we can use later
        # Pass in inputs so we can pre-populate the results cache
        prehydrated_results = {**overrides, **inputs}
        results_cache = state.DictBasedResultCache(prehydrated_results)
        # Create tasks from the grouped nodes, filtering/pruning as we go
        tasks = grouping.create_task_plan(grouped_nodes, final_vars, overrides, [fg.adapter])
        # Create a task graph and execution state
        execution_state = state.ExecutionState(tasks, results_cache)  # Stateful storage for the DAG
        # Blocking call to run through until completion
        executors.run_graph_to_completion(execution_state, self.execution_manager)
        # Read the final variables from the result cache
        raw_result = results_cache.read(final_vars)
        return self.adapter.build_result(**raw_result)


class Driver:
    """This class orchestrates creating and executing the DAG to create a dataframe.

    .. code-block:: python

        from hamilton import driver
        from hamilton import base

        # 1. Setup config or invariant input.
        config = {}

        # 2. we need to tell hamilton where to load function definitions from
        import my_functions
        # or programmatically (e.g. you can script module loading)
        module_name = 'my_functions'
        my_functions = importlib.import_module(module_name)

        # 3. Determine the return type -- default is a pandas.DataFrame.
        adapter = base.SimplePythonDataFrameGraphAdapter() # See GraphAdapter docs for more details.

        # These all feed into creating the driver & thus DAG.
        dr = driver.Driver(config, module, adapter=adapter)
    """

    def __init__(
        self,
        config: Dict[str, Any],
        *modules: ModuleType,
        adapter: base.HamiltonGraphAdapter = None,
        _graph_executor: GraphExecutor = None,
    ):
        """Constructor: creates a DAG given the configuration & modules to crawl.

        :param config: This is a dictionary of initial data & configuration.
            The contents are used to help create the DAG.
        :param modules: Python module objects you want to inspect for Hamilton Functions.
        :param adapter: Optional. A way to wire in another way of "executing" a hamilton graph.
            Defaults to using original Hamilton adapter which is single threaded in memory python.
        :param graph_executor: This is injected by the builder -- if you want to set different graph
            execution parameters, use the builder rather than instantiating this directly.
        """
        if _graph_executor is None:
            _graph_executor = DefaultGraphExecutor()
        self.graph_executor = _graph_executor

        self.driver_run_id = uuid.uuid4()
        if adapter is None:
            adapter = base.SimplePythonDataFrameGraphAdapter()
        error = None
        self.graph_modules = modules
        try:
            self.graph = graph.FunctionGraph.from_modules(*modules, config=config, adapter=adapter)
            self.adapter = adapter
        except Exception as e:
            error = telemetry.sanitize_error(*sys.exc_info())
            logger.error(SLACK_ERROR_MESSAGE)
            raise e
        finally:
            self.capture_constructor_telemetry(error, modules, config, adapter)

    def capture_constructor_telemetry(
        self,
        error: Optional[str],
        modules: Tuple[ModuleType],
        config: Dict[str, Any],
        adapter: base.HamiltonGraphAdapter,
    ):
        """Captures constructor telemetry. Notes:
        (1) we want to do this in a way that does not break.
        (2) we need to account for all possible states, e.g. someone passing in None, or assuming that
        the entire constructor code ran without issue, e.g. `adapter` was assigned to `self`.

        :param error: the sanitized error string to send.
        :param modules: the list of modules, could be None.
        :param config: the config dict passed, could be None.
        :param adapter: the adapter passed in, might not be attached to `self` yet.
        """
        if telemetry.is_telemetry_enabled():
            try:
                adapter_name = telemetry.get_adapter_name(adapter)
                result_builder = telemetry.get_result_builder_name(adapter)
                # being defensive here with ensuring values exist
                payload = telemetry.create_start_event_json(
                    len(self.graph.nodes) if hasattr(self, "graph") else 0,
                    len(modules) if modules else 0,
                    len(config) if config else 0,
                    dict(self.graph.decorator_counter) if hasattr(self, "graph") else {},
                    adapter_name,
                    result_builder,
                    self.driver_run_id,
                    error,
                    self.graph_executor.__class__.__name__,
                )
                telemetry.send_event_json(payload)
            except Exception as e:
                # we don't want this to fail at all!
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Error caught in processing telemetry: {e}")

    @staticmethod
    def validate_inputs(
        fn_graph: graph.FunctionGraph,
        adapter: base.HamiltonGraphAdapter,
        user_nodes: Collection[node.Node],
        inputs: typing.Optional[Dict[str, Any]] = None,
        nodes_set: Collection[node.Node] = None,
    ):
        """Validates that inputs meet our expectations. This means that:
        1. The runtime inputs don't clash with the graph's config
        2. All expected graph inputs are provided, either in config or at runtime

        :param user_nodes: The required nodes we need for computation.
        :param inputs: the user inputs provided.
        :param nodes_set: the set of nodes to use for validation; Optional.
        """
        if inputs is None:
            inputs = {}
        if nodes_set is None:
            nodes_set = set(fn_graph.nodes.values())
        (all_inputs,) = (graph_functions.combine_config_and_inputs(fn_graph.config, inputs),)
        errors = []
        for user_node in user_nodes:
            if user_node.name not in all_inputs:
                if graph_functions.node_is_required_by_anything(user_node, nodes_set):
                    errors.append(
                        f"Error: Required input {user_node.name} not provided "
                        f"for nodes: {[node.name for node in user_node.depended_on_by]}."
                    )
            elif all_inputs[user_node.name] is not None and not adapter.check_input_type(
                user_node.type, all_inputs[user_node.name]
            ):
                errors.append(
                    f"Error: Type requirement mismatch. Expected {user_node.name}:{user_node.type} "
                    f"got {all_inputs[user_node.name]}:{type(all_inputs[user_node.name])} instead."
                )
        if errors:
            errors.sort()
            error_str = f"{len(errors)} errors encountered:\n  " + "\n  ".join(errors)
            raise ValueError(error_str)

    def execute(
        self,
        final_vars: List[Union[str, Callable, Variable]],
        overrides: Dict[str, Any] = None,
        display_graph: bool = False,
        inputs: Dict[str, Any] = None,
    ) -> Any:
        """Executes computation.

        :param final_vars: the final list of outputs we want to compute.
        :param overrides: values that will override "nodes" in the DAG.
        :param display_graph: DEPRECATED. Whether we want to display the graph being computed.
        :param inputs: Runtime inputs to the DAG.
        :return: an object consisting of the variables requested, matching the type returned by the GraphAdapter.
            See constructor for how the GraphAdapter is initialized. The default one right now returns a pandas
            dataframe.
        """
        if display_graph:
            logger.warning(
                "display_graph=True is deprecated. It will be removed in the 2.0.0 release. "
                "Please use visualize_execution()."
            )
        start_time = time.time()
        run_successful = True
        error = None
        _final_vars = self._create_final_vars(final_vars)
        try:
            outputs = self.raw_execute(_final_vars, overrides, display_graph, inputs=inputs)
            result = self.adapter.build_result(**outputs)
            return result
        except Exception as e:
            run_successful = False
            logger.error(SLACK_ERROR_MESSAGE)
            error = telemetry.sanitize_error(*sys.exc_info())
            raise e
        finally:
            duration = time.time() - start_time
            self.capture_execute_telemetry(
                error, _final_vars, inputs, overrides, run_successful, duration
            )

    def _create_final_vars(self, final_vars: List[Union[str, Callable, Variable]]) -> List[str]:
        """Creates the final variables list - converting functions names as required.

        :param final_vars:
        :return: list of strings in the order that final_vars was provided.
        """
        _module_set = {_module.__name__ for _module in self.graph_modules}
        _final_vars = common.convert_output_values(final_vars, _module_set)
        return _final_vars

    def capture_execute_telemetry(
        self,
        error: Optional[str],
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        run_successful: bool,
        duration: float,
    ):
        """Captures telemetry after execute has run.

        Notes:
        (1) we want to be quite defensive in not breaking anyone's code with things we do here.
        (2) thus we want to double-check that values exist before doing something with them.

        :param error: the sanitized error string to capture, if any.
        :param final_vars: the list of final variables to get.
        :param inputs: the inputs to the execute function.
        :param overrides: any overrides to the execute function.
        :param run_successful: whether this run was successful.
        :param duration: time it took to run execute.
        """
        if telemetry.is_telemetry_enabled():
            try:
                payload = telemetry.create_end_event_json(
                    run_successful,
                    duration,
                    len(final_vars) if final_vars else 0,
                    len(overrides) if isinstance(overrides, Dict) else 0,
                    len(inputs) if isinstance(overrides, Dict) else 0,
                    self.driver_run_id,
                    error,
                )
                telemetry.send_event_json(payload)
            except Exception as e:
                # we don't want this to fail at all!
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Error caught in processing telemetry:\n{e}")

    def raw_execute(
        self,
        final_vars: List[str],
        overrides: Dict[str, Any] = None,
        display_graph: bool = False,
        inputs: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Raw execute function that does the meat of execute.

        It does not try to stitch anything together. Thus allowing wrapper executes around this to shape the output
        of the data.

        :param final_vars: Final variables to compute
        :param overrides: Overrides to run.
        :param display_graph: DEPRECATED. DO NOT USE. Whether or not to display the graph when running it
        :param inputs: Runtime inputs to the DAG
        :return:
        """
        nodes, user_nodes = self.graph.get_upstream_nodes(final_vars, inputs, overrides)
        Driver.validate_inputs(
            self.graph, self.adapter, user_nodes, inputs, nodes
        )  # TODO -- validate within the function graph itself
        if display_graph:  # deprecated flow.
            logger.warning(
                "display_graph=True is deprecated. It will be removed in the 2.0.0 release. "
                "Please use visualize_execution()."
            )
            self.visualize_execution(final_vars, "test-output/execute.gv", {"view": True})
            if self.has_cycles(final_vars):  # here for backwards compatible driver behavior.
                raise ValueError("Error: cycles detected in your graph.")
        all_nodes = nodes | user_nodes
        self.graph_executor.validate(list(all_nodes))
        return self.graph_executor.execute(
            self.graph,
            final_vars,
            overrides if overrides is not None else {},
            inputs if inputs is not None else {},
        )

    @capture_function_usage
    def list_available_variables(self) -> List[Variable]:
        """Returns available variables, i.e. outputs.

        These variables corresond 1:1 with nodes in the DAG, and contain the following information:
        1. name: the name of the node
        2. tags: the tags associated with this node
        3. type: The type of data this node returns
        4. is_external_input: Whether this node represents an external input (required from outside),
        or not (has a function specifying its behavior).


        :return: list of available variables (i.e. outputs).
        """
        return [Variable.from_node(n) for n in self.graph.get_nodes()]

    @capture_function_usage
    def display_all_functions(
        self, output_file_path: str = None, render_kwargs: dict = None, graphviz_kwargs: dict = None
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Displays the graph of all functions loaded!

        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph-all.dot'. Optional. No need to pass it in if you're in a Jupyter Notebook.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
            See https://graphviz.readthedocs.io/en/stable/api.html#graphviz.Graph.render for other options.
        :param graphviz_kwargs: Optional. Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
            See https://graphviz.org/doc/info/attrs.html for options.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        try:
            return self.graph.display_all(output_file_path, render_kwargs, graphviz_kwargs)
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @staticmethod
    def _visualize_execution_helper(
        fn_graph: graph.FunctionGraph,
        adapter: base.HamiltonGraphAdapter,
        final_vars: List[str],
        output_file_path: str,
        render_kwargs: dict,
        inputs: Dict[str, Any] = None,
        graphviz_kwargs: dict = None,
        overrides: Dict[str, Any] = None,
    ):
        """Helper function to visualize execution, using a passed-in function graph.

        :param final_vars:
        :param output_file_path:
        :param render_kwargs:
        :param inputs:
        :param graphviz_kwargs:
        :return:
        """
        nodes, user_nodes = fn_graph.get_upstream_nodes(final_vars, inputs, overrides)
        Driver.validate_inputs(fn_graph, adapter, user_nodes, inputs, nodes)
        node_modifiers = {fv: {graph.VisualizationNodeModifiers.IS_OUTPUT} for fv in final_vars}
        for user_node in user_nodes:
            if user_node.name not in node_modifiers:
                node_modifiers[user_node.name] = set()
            node_modifiers[user_node.name].add(graph.VisualizationNodeModifiers.IS_USER_INPUT)
        all_nodes = nodes | user_nodes
        if overrides is not None:
            for node_ in all_nodes:
                if node_.name in overrides:
                    # We don't want to display it if we're overriding it
                    # This is necessary as getting upstream nodes includes overrides
                    if node_.name not in node_modifiers:
                        node_modifiers[node_.name] = set()
                    node_modifiers[node_.name].add(graph.VisualizationNodeModifiers.IS_OVERRIDE)
        try:
            return fn_graph.display(
                all_nodes,
                output_file_path=output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                node_modifiers=node_modifiers,
                strictly_display_only_passed_in_nodes=True,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def visualize_execution(
        self,
        final_vars: List[Union[str, Callable, Variable]],
        output_file_path: str = None,
        render_kwargs: dict = None,
        inputs: Dict[str, Any] = None,
        graphviz_kwargs: dict = None,
        overrides: Dict[str, Any] = None,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Visualizes Execution.

        Note: overrides are not handled at this time.

        Shapes:

         - ovals are nodes/functions
         - rectangles are nodes/functions that are requested as output
         - shapes with dotted lines are inputs required to run the DAG.

        :param final_vars: the outputs we want to compute. They will become rectangles in the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Optional. No need to pass it in if you're in a Jupyter Notebook.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
            See https://graphviz.readthedocs.io/en/stable/api.html#graphviz.Graph.render for other options.
        :param inputs: Optional. Runtime inputs to the DAG.
        :param graphviz_kwargs: Optional. Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
            See https://graphviz.org/doc/info/attrs.html for options.
        :param overrides: Optional. Overrides to the DAG.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        _final_vars = self._create_final_vars(final_vars)
        return self._visualize_execution_helper(
            self.graph,
            self.adapter,
            _final_vars,
            output_file_path,
            render_kwargs,
            inputs,
            graphviz_kwargs,
            overrides,
        )

    @capture_function_usage
    def has_cycles(self, final_vars: List[Union[str, Callable, Variable]]) -> bool:
        """Checks that the created graph does not have cycles.

        :param final_vars: the outputs we want to compute.
        :return: boolean True for cycles, False for no cycles.
        """
        _final_vars = self._create_final_vars(final_vars)
        # get graph we'd be executing over
        nodes, user_nodes = self.graph.get_upstream_nodes(_final_vars)
        return self.graph.has_cycles(nodes, user_nodes)

    @capture_function_usage
    def what_is_downstream_of(self, *node_names: str) -> List[Variable]:
        """Tells you what is downstream of this function(s), i.e. node(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :return: list of "variables" (i.e. nodes), inclusive of the function names, that are downstream of the passed
                in function names.
        """
        downstream_nodes = self.graph.get_downstream_nodes(list(node_names))
        return [Variable.from_node(n) for n in downstream_nodes]

    @capture_function_usage
    def display_downstream_of(
        self,
        *node_names: str,
        output_file_path: str = None,
        render_kwargs: dict = None,
        graphviz_kwargs: dict = None,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Creates a visualization of the DAG starting from the passed in function name(s).

        Note: for any "node" visualized, we will also add its parents to the visualization as well, so
        there could be more nodes visualized than strictly what is downstream of the passed in function name(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Optional. No need to pass it in if you're in a Jupyter Notebook.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        downstream_nodes = self.graph.get_downstream_nodes(list(node_names))
        try:
            return self.graph.display(
                downstream_nodes,
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                strictly_display_only_passed_in_nodes=False,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def display_upstream_of(
        self,
        *node_names: str,
        output_file_path: str = None,
        render_kwargs: dict = None,
        graphviz_kwargs: dict = None,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Creates a visualization of the DAG going backwards from the passed in function name(s).

        Note: for any "node" visualized, we will also add its parents to the visualization as well, so
        there could be more nodes visualized than strictly what is downstream of the passed in function name(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Optional. No need to pass it in if you're in a Jupyter Notebook.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`. Optional.
        :param graphviz_kwargs: Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image. Optional.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        upstream_nodes, user_nodes = self.graph.get_upstream_nodes(list(node_names))
        node_modifiers = {}
        for n in user_nodes:
            node_modifiers[n.name] = {graph.VisualizationNodeModifiers.IS_USER_INPUT}
        try:
            return self.graph.display(
                upstream_nodes,
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                strictly_display_only_passed_in_nodes=False,
                node_modifiers=node_modifiers,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def what_is_upstream_of(self, *node_names: str) -> List[Variable]:
        """Tells you what is upstream of this function(s), i.e. node(s).

        :param node_names: names of function(s) that are starting points for traversing the graph backwards.
        :return: list of "variables" (i.e. nodes), inclusive of the function names, that are upstream of the passed
                in function names.
        """
        upstream_nodes, _ = self.graph.get_upstream_nodes(list(node_names))
        return [Variable.from_node(n) for n in upstream_nodes]

    @capture_function_usage
    def what_is_the_path_between(
        self, upstream_node_name: str, downstream_node_name: str
    ) -> List[Variable]:
        """Tells you what nodes are on the path between two nodes.

        Note: this is inclusive of the two nodes, and returns an unsorted list of nodes.

        :param upstream_node_name: the name of the node that we want to start from.
        :param downstream_node_name: the name of the node that we want to end at.
        :return: Nodes representing the path between the two nodes, inclusive of the two nodes, unsorted.
            Returns empty list if no path exists.
        :raise ValueError: if the upstream or downstream node name is not in the graph.
        """
        all_variables = {n.name: n for n in self.graph.get_nodes()}
        # ensure that the nodes exist
        if upstream_node_name not in all_variables:
            raise ValueError(f"Upstream node {upstream_node_name} not found in graph.")
        if downstream_node_name not in all_variables:
            raise ValueError(f"Downstream node {downstream_node_name} not found in graph.")
        nodes_for_path = self._get_nodes_between(upstream_node_name, downstream_node_name)
        return [Variable.from_node(n) for n in nodes_for_path]

    def _get_nodes_between(
        self, upstream_node_name: str, downstream_node_name: str
    ) -> Set[node.Node]:
        """Gets the nodes representing the path between two nodes, inclusive of the two nodes.

        Assumes that the nodes exist in the graph.

        :param upstream_node_name: the name of the node that we want to start from.
        :param downstream_node_name: the name of the node that we want to end at.
        :return: set of nodes that comprise the path between the two nodes, inclusive of the two nodes.
        """
        downstream_nodes = self.graph.get_downstream_nodes([upstream_node_name])
        # we skip user_nodes because it'll be the upstream node, or it wont matter.
        upstream_nodes, _ = self.graph.get_upstream_nodes([downstream_node_name])
        nodes_for_path = set(downstream_nodes).intersection(set(upstream_nodes))
        return nodes_for_path

    @capture_function_usage
    def visualize_path_between(
        self,
        upstream_node_name: str,
        downstream_node_name: str,
        output_file_path: Optional[str] = None,
        render_kwargs: dict = None,
        graphviz_kwargs: dict = None,
        strict_path_visualization: bool = False,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Visualizes the path between two nodes.

        This is useful for debugging and understanding the path between two nodes.

        :param upstream_node_name: the name of the node that we want to start from.
        :param downstream_node_name: the name of the node that we want to end at.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Pass in None to skip saving any file.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        :param strict_path_visualization: If True, only the nodes in the path will be visualized. If False, the
            nodes in the path and their dependencies, i.e. parents, will be visualized.
        :return: graphviz object.
        :raise ValueError: if the upstream or downstream node names are not found in the graph,
            or there is no path between them.
        """
        if render_kwargs is None:
            render_kwargs = {}
        if graphviz_kwargs is None:
            graphviz_kwargs = {}
        all_variables = {n.name: n for n in self.graph.get_nodes()}
        # ensure that the nodes exist
        if upstream_node_name not in all_variables:
            raise ValueError(f"Upstream node {upstream_node_name} not found in graph.")
        if downstream_node_name not in all_variables:
            raise ValueError(f"Downstream node {downstream_node_name} not found in graph.")

        # set whether the node is user input
        node_modifiers = {}
        for n in self.graph.get_nodes():
            if n.user_defined:
                node_modifiers[n.name] = {graph.VisualizationNodeModifiers.IS_USER_INPUT}

        # create nodes that constitute the path
        nodes_for_path = self._get_nodes_between(upstream_node_name, downstream_node_name)
        if len(nodes_for_path) == 0:
            raise ValueError(
                f"No path found between {upstream_node_name} and {downstream_node_name}."
            )
        # add is path for node_modifier's dict
        for n in nodes_for_path:
            if n.name not in node_modifiers:
                node_modifiers[n.name] = set()
            node_modifiers[n.name].add(graph.VisualizationNodeModifiers.IS_PATH)
        try:
            return self.graph.display(
                nodes_for_path,
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                node_modifiers=node_modifiers,
                strictly_display_only_passed_in_nodes=strict_path_visualization,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def materialize(
        self,
        *materializers: materialization.MaterializerFactory,
        additional_vars: List[Union[str, Callable, Variable]] = None,
        overrides: Dict[str, Any] = None,
        inputs: Dict[str, Any] = None,
    ) -> Tuple[Any, Dict[str, Any]]:
        """Executes and materializes with ad-hoc materializers.This does the following:
        1. Creates a new graph, appending the desired materialization nodes
        2. Runs the graph with the materialization nodes outputted, as well as any additional
        nodes requested (which can be empty)
        3. Returns a Tuple[Materialization metadata, additional_vars result]

        For instance, say you want to Materialize the output of a node to CSV:

        .. code-block:: python

             from hamilton import driver, base
             from hamilton.io.materialization import to
             dr = driver.Driver(my_module, {})
             # foo, bar are pd.Series
             metadata, result = dr.Materialize(
                 to.csv(
                     path="./output.csv"
                     id="foo_bar_csv",
                     dependencies=["foo", "bar"],
                     combine=base.PandasDataFrameResult()
                 ),
                 additional_vars=["foo", "bar"]
             )

        The code above will Materialize the dataframe with "foo" and "bar" as columns, saving it as
        a CSV file at "./output.csv". The metadata will contain any additional relevant information,
        and result will be a dictionary with the keys "foo" and "bar" containing the original data.

        Note that we pass in a `ResultBuilder` as the `combine` argument, as we may be materializing
        several nodes.

        additional_vars is used for debugging -- E.G. if you want to both realize side-effects and
        return an output for inspection. If left out, it will return an empty dictionary.

        You can bypass the `combine` keyword if only one output is required. In this circumstance "combining/joining" isn't required, e.g. you do that yourself in a function and/or the output of the function
        can be directly used. In the case below the output can be turned in to a CSV.

        .. code-block:: python

             from hamilton import driver, base
             from hamilton.io.materialization import to
             dr = driver.Driver(my_module, {})
             # foo, bar are pd.Series
             metadata, _ = dr.Materialize(
                 to.csv(
                     path="./output.csv"
                     id="foo_bar_csv",
                     dependencies=["foo_bar_already_joined],
                 ),
             )

        This will just save it to a csv.

        Note that materializers can be any valid DataSaver -- these have an isomorphic relationship
        with the `@save_to` decorator, which means that any key utilizable in `save_to` can be used
        in a materializer. The constructor arguments for a materializer are the same as the
        arguments for `@save_to`, with an additional trick -- instead of requiring
        everything to be a `source` or `value`, you can pass in a literal, and it will be interpreted
        as a value.

        That said, if you want to parameterize your materializer based on input or some node in the
        DAG, you can easily do that as well:

        .. code-block:: python

             from hamilton import driver, base
             from hamilton.function_modifiers import source
             from hamilton.io.materialization import to
             dr = driver.Driver(my_module, {})
             # foo, bar are pd.Series
             metadata, result = dr.Materialize(
                 to.csv(
                     path=source("save_path"),
                     id="foo_bar_csv",
                     dependencies=["foo", "bar"],
                     combine=base.PandasDataFrameResult()
                 ),
                 additional_vars=["foo", "bar"],
                 inputs={"save_path" : "./output.csv"}
             )

        While this is a contrived example, you could imagine something more powerful. Say, for
        instance, say you have created and registered a custom `model_registry` materializer that
        applies to an argument of your model class, and requires `training_data` to infer the
        signature. You could call it like this:

        .. code-block:: python

            from hamilton import driver, base
            from hamilton.function_modifiers import source
            from hamilton.io.materialization import to
            dr = driver.Driver(my_module, {})
            metadata, _ = dr.Materialize(
                to.model_registry(
                    training_data=source("training_data"),
                    id="foo_model_registry",
                    dependencies=["foo_model"]
                ),
            )

        In this case, we bypass a result builder (as there's only one model), the single
        node we depend on gets saved, and we pass in the training data as an input so the
        materializer can infer the signature

        This is customizable through two APIs:
            1. Custom data savers ( :doc:`/concepts/decorators-overview`
            2. Custom result builders

        If you find yourself writing these, please consider contributing back! We would love
        to round out the set of available materialization tools.

        If you find yourself writing these, please consider contributing back! We would love
        to round out the set of available materialization tools.

        :param materializers: Materializer factories, created with Materialize.<output_type>
        :param additional_vars: Additional variables to return from the graph
        :param overrides: Overrides to pass to execution
        :param inputs: Inputs to pass to execution
        :return: Tuple[Materialization metadata|data, additional_vars result]
        """
        if additional_vars is None:
            additional_vars = []
        start_time = time.time()
        run_successful = True
        error = None

        final_vars = self._create_final_vars(additional_vars)
        materializer_vars = [materializer.id for materializer in materializers]
        try:
            module_set = {_module.__name__ for _module in self.graph_modules}
            materializers = [m.sanitize_dependencies(module_set) for m in materializers]
            function_graph = materialization.modify_graph(self.graph, materializers)
            # need to validate the right inputs has been provided.
            # we do this on the modified graph.
            nodes, user_nodes = function_graph.get_upstream_nodes(
                final_vars + materializer_vars, inputs, overrides
            )
            Driver.validate_inputs(function_graph, self.adapter, user_nodes, inputs, nodes)
            all_nodes = nodes | user_nodes
            self.graph_executor.validate(list(all_nodes))
            raw_results = self.graph_executor.execute(
                function_graph,
                final_vars=final_vars + materializer_vars,
                overrides=overrides,
                inputs=inputs,
            )
            materialization_output = {key: raw_results[key] for key in materializer_vars}
            raw_results_output = {key: raw_results[key] for key in final_vars}

            return materialization_output, raw_results_output
        except Exception as e:
            run_successful = False
            logger.error(SLACK_ERROR_MESSAGE)
            error = telemetry.sanitize_error(*sys.exc_info())
            raise e
        finally:
            duration = time.time() - start_time
            self.capture_execute_telemetry(
                error, final_vars + materializer_vars, inputs, overrides, run_successful, duration
            )

    @capture_function_usage
    def visualize_materialization(
        self,
        *materializers: materialization.MaterializerFactory,
        output_file_path: str = None,
        render_kwargs: dict = None,
        additional_vars: List[Union[str, Callable, Variable]] = None,
        inputs: Dict[str, Any] = None,
        graphviz_kwargs: dict = None,
        overrides: Dict[str, Any] = None,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Visualizes materialization. This helps give you a sense of how materialization
        will impact the DAG.

        :param materializers: Materializers to use, see the materialize() function
        :param additional_vars: Additional variables to compute (in addition to materializers)
        :param output_file_path: Path to output file. Optional. Skip if in a Jupyter Notebook.
        :param render_kwargs: Arguments to pass to render. Optional.
        :param inputs: Inputs to pass to execution. Optional.
        :param graphviz_kwargs: Arguments to pass to graphviz. Optional.
        :param overrides: Overrides to pass to execution. Optional.
        :return: The graphviz graph, if you want to do something with it
        """
        if additional_vars is None:
            additional_vars = []

        module_set = {_module.__name__ for _module in self.graph_modules}
        materializers = [m.sanitize_dependencies(module_set) for m in materializers]
        function_graph = materialization.modify_graph(self.graph, materializers)
        _final_vars = self._create_final_vars(additional_vars) + [
            materializer.id for materializer in materializers
        ]
        return Driver._visualize_execution_helper(
            function_graph,
            self.adapter,
            _final_vars,
            output_file_path,
            render_kwargs,
            inputs,
            graphviz_kwargs,
            overrides,
        )


class Builder:
    def __init__(self):
        """Constructs a driver builder. No parameters as you call methods to set fields."""
        # Toggling versions
        self.v2_executor = False

        # common fields
        self.config = {}
        self.modules = []
        # V1 fields
        self.adapter = None

        # V2 fields
        self.execution_manager = None
        self.local_executor = None
        self.remote_executor = None
        self.grouping_strategy = None
        self.result_builder = None

    def _require_v2(self, message: str):
        if not self.v2_executor:
            raise ValueError(message)

    def _require_field_unset(self, field: str, message: str, unset_value: Any = None):
        if getattr(self, field) != unset_value:
            raise ValueError(message)

    def _require_field_set(self, field: str, message: str, unset_value: Any = None):
        if getattr(self, field) == unset_value:
            raise ValueError(message)

    def enable_dynamic_execution(self, *, allow_experimental_mode: bool = False) -> "Builder":
        """Enables the Parallelizable[] type, which in turn enables:
        1. Grouped execution into tasks
        2. Parallel execution
        :return: self
        """
        if not allow_experimental_mode:
            raise ValueError(
                "Remote execution is currently experimental. "
                "Please set allow_experiemental_mode=True to enable it."
            )
        self._require_field_unset("adapter", "Cannot enable remote execution with an adapter set.")
        self.v2_executor = True
        return self

    def with_config(self, config: Dict[str, Any]) -> "Builder":
        """Adds the specified configuration to the config.
        This can be called multilple times -- later calls will take precedence.

        :param config: Config to use.
        :return: self
        """
        self.config.update(config)
        return self

    def with_modules(self, *modules: ModuleType) -> "Builder":
        """Adds the specified modules to the modules list.
        This can be called multiple times -- later calls will take precedence.

        :param modules: Modules to use.
        :return: self
        """
        self.modules.extend(modules)
        return self

    def with_adapter(self, adapter: base.HamiltonGraphAdapter) -> "Builder":
        """Sets the adapter to use.

        :param adapter: Adapter to use.
        :return: self
        """
        self._require_field_unset("adapter", "Cannot set adapter twice.")
        self.adapter = adapter
        return self

    def with_execution_manager(self, execution_manager: executors.ExecutionManager) -> "Builder":
        """Sets the execution manager to use. Note that this cannot be used if local_executor
        or remote_executor are also set

        :param execution_manager:
        :return: self
        """
        self._require_v2("Cannot set execution manager without first enabling the V2 Driver")
        self._require_field_unset("execution_manager", "Cannot set execution manager twice")
        self._require_field_unset(
            "remote_executor",
            "Cannot set execution manager with remote " "executor set -- these are disjoint",
        )

        self.execution_manager = execution_manager
        return self

    def with_remote_executor(self, remote_executor: executors.TaskExecutor) -> "Builder":
        """Sets the execution manager to use. Note that this cannot be used if local_executor
        or remote_executor are also set

        :param remote_executor: Remote executor to use
        :return: self
        """
        self._require_v2("Cannot set execution manager without first enabling the V2 Driver")
        self._require_field_unset("remote_executor", "Cannot set remote executor twice")
        self._require_field_unset(
            "execution_manager",
            "Cannot set remote executor with execution " "manager set -- these are disjoint",
        )
        self.remote_executor = remote_executor
        return self

    def with_local_executor(self, local_executor: executors.TaskExecutor) -> "Builder":
        """Sets the execution manager to use. Note that this cannot be used if local_executor
        or remote_executor are also set

        :param local_executor: Local executor to use
        :return: self
        """
        self._require_v2("Cannot set execution manager without first enabling the V2 Driver")
        self._require_field_unset("local_executor", "Cannot set local executor twice")
        self._require_field_unset(
            "execution_manager",
            "Cannot set local executor with execution " "manager set -- these are disjoint",
        )
        self.local_executor = local_executor
        return self

    def with_grouping_strategy(self, grouping_strategy: grouping.GroupingStrategy) -> "Builder":
        """Sets a node grouper, which tells the driver how to group nodes into tasks for execution.

        :param node_grouper: Node grouper to use.
        :return: self
        """
        self._require_v2("Cannot set grouping strategy without first enabling the V2 Driver")
        self._require_field_unset("grouping_strategy", "Cannot set grouping strategy twice")
        self.grouping_strategy = grouping_strategy
        return self

    def build(self) -> Driver:
        """Builds the driver -- note that this can return a different class, so you'll likely
        want to have a sense of what it returns.

        Note: this defaults to a dictionary adapter if no adapter is set.

        :return: The driver you specified.
        """
        adapter = self.adapter if self.adapter is not None else base.DefaultAdapter()
        if not self.v2_executor:
            return Driver(self.config, *self.modules, adapter=adapter)
        execution_manager = self.execution_manager
        if execution_manager is None:
            local_executor = self.local_executor or executors.SynchronousLocalTaskExecutor()
            remote_executor = self.remote_executor or executors.MultiThreadingExecutor(max_tasks=10)
            execution_manager = executors.DefaultExecutionManager(
                local_executor=local_executor, remote_executor=remote_executor
            )
        grouping_strategy = self.grouping_strategy or grouping.GroupByRepeatableBlocks()
        graph_executor = TaskBasedGraphExecutor(
            execution_manager=execution_manager,
            grouping_strategy=grouping_strategy,
            adapter=adapter,
        )
        return Driver(
            self.config,
            *self.modules,
            adapter=adapter,
            _graph_executor=graph_executor,
        )


if __name__ == "__main__":
    """some example test code"""
    import importlib

    formatter = logging.Formatter("[%(levelname)s] %(asctime)s %(name)s(%(lineno)s): %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

    if len(sys.argv) < 2:
        logger.error("No modules passed")
        sys.exit(1)
    logger.info(f"Importing {sys.argv[1]}")
    module = importlib.import_module(sys.argv[1])

    x = pd.date_range("2019-01-05", "2020-12-31", freq="7D")
    x.index = x

    dr = Driver(
        {
            "VERSION": "kids",
            "as_of": datetime.strptime("2019-06-01", "%Y-%m-%d"),
            "end_date": "2020-12-31",
            "start_date": "2019-01-05",
            "start_date_d": datetime.strptime("2019-01-05", "%Y-%m-%d"),
            "end_date_d": datetime.strptime("2020-12-31", "%Y-%m-%d"),
            "segment_filters": {"business_line": "womens"},
        },
        module,
    )
    df = dr.execute(
        ["date_index", "some_column"]
        # ,overrides={'DATE': pd.Series(0)}
        ,
        display_graph=False,
    )
    print(df)
