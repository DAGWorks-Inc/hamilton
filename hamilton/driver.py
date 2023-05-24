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


class Driver(object):
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
    ):
        """Constructor: creates a DAG given the configuration & modules to crawl.

        :param config: This is a dictionary of initial data & configuration.
                       The contents are used to help create the DAG.
        :param modules: Python module objects you want to inspect for Hamilton Functions.
        :param adapter: Optional. A way to wire in another way of "executing" a hamilton graph.
                        Defaults to using original Hamilton adapter which is single threaded in memory python.
        """
        self.driver_run_id = uuid.uuid4()
        if adapter is None:
            adapter = base.SimplePythonDataFrameGraphAdapter()
        error = None
        self.graph_modules = modules
        try:
            self.graph = graph.FunctionGraph(*modules, config=config, adapter=adapter)
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
        """Captures constructor telemetry.

        Notes:
        (1) we want to do this in a way that does not break.
        (2) we need to account for all possible states, e.g. someone passing in None, or assuming that
        the entire constructor code ran without issue, e.g. `adpater` was assigned to `self`.

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
                )
                telemetry.send_event_json(payload)
            except Exception as e:
                # we don't want this to fail at all!
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Error caught in processing telemetry: {e}")

    def _node_is_required_by_anything(self, node_: node.Node, node_set: Set[node.Node]) -> bool:
        """Checks dependencies on this node and determines if at least one requires it.

        Nodes can be optionally depended upon, i.e. the function parameter has a default value. We want to check that
        of the nodes the depend on this one, at least one of them requires it, i.e. the parameter is not optional.

        :param node_: node in question
        :param node_set: checks that we traverse only nodes in the provided set.
        :return: True if it is required by any downstream node, false otherwise
        """
        required = False
        for downstream_node in node_.depended_on_by:
            if downstream_node not in node_set:
                continue
            _, dep_type = downstream_node.input_types[node_.name]
            if dep_type == node.DependencyType.REQUIRED:
                return True
        return required

    def validate_inputs(
        self,
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
            nodes_set = set(self.graph.nodes.values())
        (all_inputs,) = (graph.FunctionGraph.combine_config_and_inputs(self.graph.config, inputs),)
        errors = []
        for user_node in user_nodes:
            if user_node.name not in all_inputs:
                if self._node_is_required_by_anything(user_node, nodes_set):
                    errors.append(
                        f"Error: Required input {user_node.name} not provided "
                        f"for nodes: {[node.name for node in user_node.depended_on_by]}."
                    )
            elif all_inputs[user_node.name] is not None and not self.adapter.check_input_type(
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
        _final_vars = []
        errors = []
        module_set = {_module.__name__ for _module in self.graph_modules}
        for final_var in final_vars:
            if isinstance(final_var, str):
                _final_vars.append(final_var)
            elif isinstance(final_var, Variable):
                _final_vars.append(final_var.name)
            elif isinstance(final_var, Callable):
                if final_var.__module__ in module_set:
                    _final_vars.append(final_var.__name__)
                else:
                    errors.append(
                        f"Function {final_var.__module__}.{final_var.__name__} is a function not in a "
                        f"module given to the driver. Valid choices are {module_set}."
                    )
            else:
                errors.append(
                    f"Final var {final_var} is not a string, a function, or a driver.Variable."
                )
        if errors:
            errors.sort()
            error_str = f"{len(errors)} errors encountered:\n  " + "\n  ".join(errors)
            raise ValueError(error_str)
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
        nodes, user_nodes = self.graph.get_upstream_nodes(final_vars, inputs)
        self.validate_inputs(
            user_nodes, inputs, nodes
        )  # TODO -- validate within the function graph itself
        if display_graph:  # deprecated flow.
            logger.warning(
                "display_graph=True is deprecated. It will be removed in the 2.0.0 release. "
                "Please use visualize_execution()."
            )
            self.visualize_execution(final_vars, "test-output/execute.gv", {"view": True})
            if self.has_cycles(final_vars):  # here for backwards compatible driver behavior.
                raise ValueError("Error: cycles detected in you graph.")
        memoized_computation = dict()  # memoized storage
        self.graph.execute(nodes, memoized_computation, overrides, inputs)
        outputs = {
            c: memoized_computation[c] for c in final_vars
        }  # only want request variables in df.
        del memoized_computation  # trying to cleanup some memory
        return outputs

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
        self, output_file_path: str, render_kwargs: dict = None, graphviz_kwargs: dict = None
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Displays the graph of all functions loaded!

        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph-all.dot'
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

    @capture_function_usage
    def visualize_execution(
        self,
        final_vars: List[Union[str, Callable, Variable]],
        output_file_path: str,
        render_kwargs: dict,
        inputs: Dict[str, Any] = None,
        graphviz_kwargs: dict = None,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Visualizes Execution.

        Note: overrides are not handled at this time.

        Shapes:

         - ovals are nodes/functions
         - rectangles are nodes/functions that are requested as output
         - shapes with dotted lines are inputs required to run the DAG.

        :param final_vars: the outputs we want to compute. They will become rectangles in the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
            See https://graphviz.readthedocs.io/en/stable/api.html#graphviz.Graph.render for other options.
        :param inputs: Optional. Runtime inputs to the DAG.
        :param graphviz_kwargs: Optional. Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
            See https://graphviz.org/doc/info/attrs.html for options.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        _final_vars = self._create_final_vars(final_vars)
        nodes, user_nodes = self.graph.get_upstream_nodes(_final_vars, inputs)
        self.validate_inputs(user_nodes, inputs, nodes)
        node_modifiers = {fv: {graph.VisualizationNodeModifiers.IS_OUTPUT} for fv in _final_vars}
        for user_node in user_nodes:
            if user_node.name not in node_modifiers:
                node_modifiers[user_node.name] = set()
            node_modifiers[user_node.name].add(graph.VisualizationNodeModifiers.IS_USER_INPUT)
        try:
            return self.graph.display(
                nodes.union(user_nodes),
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                node_modifiers=node_modifiers,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

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
        self, *node_names: str, output_file_path: str, render_kwargs: dict, graphviz_kwargs: dict
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Creates a visualization of the DAG starting from the passed in function name(s).

        Note: for any "node" visualized, we will also add its parents to the visualization as well, so
        there could be more nodes visualized than strictly what is downstream of the passed in function name(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Pass in None to skip saving any file.
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
        self, *node_names: str, output_file_path: str, render_kwargs: dict, graphviz_kwargs: dict
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Creates a visualization of the DAG going backwards from the passed in function name(s).

        Note: for any "node" visualized, we will also add its parents to the visualization as well, so
        there could be more nodes visualized than strictly what is downstream of the passed in function name(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Pass in None to skip saving any file.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
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
