from types import ModuleType
from typing import Any, Callable, Collection, Dict, List, Tuple, Type, Union

# Copied this over from function_graph
# TODO -- determine the best place to put this code
from hamilton import graph_utils, node
from hamilton.function_modifiers import base, dependencies
from hamilton.function_modifiers.base import InvalidDecoratorException


def assign_namespace(node_name: str, namespace: str) -> str:
    return f"{namespace}.{node_name}"


def derive_type(dependency: dependencies.LiteralDependency):
    """Quick hack to derive the type of a static dependency.
    We might want to consider the type provided by the function that needs it.
    Or we can use the subclass checker/whatnot in function_graph
    (althoyugh we'll want to move it out)

    :param dependency: Dependency on which
    :return: The type of the dependency
    """
    return type(dependency.value)


def create_identity_node(from_: str, typ: Type[Type], name: str, namespace: Tuple[str, ...]):
    """Creates an identity node -- this passes through the exact
    value returned by the upstream node.

    :param from_: Source node
    :param typ: Type of the input node
    :param name: Name of the final node to create
    :param namespace: Namespace of the node
    :return: A node that simply copies the source node
    """

    def identity(**kwargs):
        return list(kwargs.values())[0]  # Maybe come up with a better way to do this

    return node.Node(
        name=name,
        typ=typ,
        doc_string="",
        callabl=identity,
        input_types={from_: typ},
        namespace=namespace
        # TODO -- add tags?
    )


def extract_all_known_types(nodes: Collection[node.Node]) -> Dict[str, Type[Type]]:
    """Extracts all known types from a set of nodes given the dependencies.
    We have to do this as we don't know the dependency types at compile-time of
    upstream nodes. That said, this is only used for guessing dependency types of
    identity nodes. In which case, we probably want some sort of sentinel "pass-through"
    dependency type that handles this better. But, for now, we'll derive it from the
    dependencies we've seen.

    :param nodes: nodes to look through for dependencies
    :return: A dictionary of all known types.
    """
    observed_types = {}
    for node_ in nodes:
        for dep_name, (type_, _) in node_.input_types.items():
            observed_types[dep_name] = type_
    return observed_types


def create_static_node(typ: Type, name: str, value: Any, namespace: Tuple[str, ...]) -> node.Node:
    """Utility function to create a static node -- this helps us bridge nodes together.

    :param typ: Type of the node to create
    :param name: Name of the node to create
    :param value: Value that the node's function always returns
    :param namespace: Namespace of the node
    :return: The instantiated static node
    """

    def node_fn(_value=value):
        return _value

    return node.Node(name=name, typ=typ, callabl=node_fn, input_types={}, namespace=namespace)


class subdag(base.NodeCreator):
    def __init__(
        self,
        *load_from: Union[ModuleType, Callable],
        inputs: Dict[
            str, Union[dependencies.ParametrizedDependency, dependencies.LiteralDependency]
        ],
        config: Dict[str, Any] = None,
    ):
        """Initializes a replay decorator. This decorator replays a subdag with a specified configuration.

        :param load_from: The functions that will be used to generate this subDAG
        :param namespace: Namespace with which to prefict nodes
        :param with_inputs: Parameterized dependencies to inject into all sources of this subDAG.
        This should *not* be an intermediate node in the subDAG.
        :param outputs: A dictionary of original node name -> output node name that forms the output of this DAG.
        :param with_config: A configuration dictionary for *just* this subDAG. Note that this passed in value takes precedence.
        """
        self.subdag_functions = subdag.collect_functions(load_from)
        self.inputs = inputs if inputs is not None else {}
        self.config = config if config is not None else {}
        self._validate_config_inputs(config, inputs)

    def _validate_config_inputs(self, config: Dict[str, Any], inputs: Dict[str, Any]):
        """Validates that the inputs specified in the config are valid.

        :param original_config: Original configuration
        :return: None
        """
        # TODO -- implement this
        shared_keys = set(config.keys()).intersection(set(inputs.keys()))
        if shared_keys:
            raise InvalidDecoratorException(
                f"Config keys {shared_keys} are shared with inputs. This is not allowed."
                f"Instead, please specify the inputs you need *just* as part of the config. "
                f"That way, you only write them once! Or, if you don't need them as a config item,"
                f"just use them in inputs."
            )

    @staticmethod
    def collect_functions(
        load_from: Union[Collection[ModuleType], Collection[Callable]]
    ) -> List[Callable]:
        """Utility function to collect functions from a list of callables/modules.

        :param load_from: A list of callables or modules to load from
        :return: a list of callables to use to create a DAG.
        """
        if len(load_from) == 0:
            raise ValueError(f"No functions were passed to {subdag.__name__}(load_from=...)")
        out = []
        for item in load_from:
            if isinstance(item, Callable):
                out.append(item)
            out.extend(
                [function for _, function in graph_utils.find_functions(function_module=item)]
            )
        return out

    def _collect_nodes(self, original_config: Dict[str, Any]):
        combined_config = dict(original_config, **self.config)
        nodes = []
        for fn in self.subdag_functions:
            nodes.extend(base.resolve_nodes(fn, combined_config))
        return nodes

    def _create_additional_static_nodes(
        self, nodes: Collection[node.Node], namespace: str
    ) -> Collection[node.Node]:
        # These already have the namespace on them
        # This allows us to inject values into the replayed subdag
        node_types = extract_all_known_types(nodes)
        out = []
        for key, value in self.inputs.items():
            # TODO -- fix type derivation. Currently we don't use the specified type as we don't really know what it should be...
            new_node_name = assign_namespace(key, namespace)
            if value.get_dependency_type() == dependencies.ParametrizedDependencySource.LITERAL:
                out.append(
                    create_static_node(
                        typ=derive_type(value),
                        name=key,
                        value=value.value,
                        namespace=(namespace,),
                    )
                )
            elif value.get_dependency_type() == dependencies.ParametrizedDependencySource.UPSTREAM:
                out.append(
                    create_identity_node(
                        from_=value.source,
                        typ=node_types[new_node_name],
                        name=key,
                        namespace=(namespace,),
                    )
                )
        for key, value in self.config.items():
            out.append(
                create_static_node(
                    typ=type(value),
                    name=key,
                    value=value,
                    namespace=(namespace,),
                )
            )
        return out

    def _add_namespace(self, nodes: List[node.Node], namespace: str) -> List[node.Node]:
        """Utility function to add a namespace to nodes.

        :param nodes:
        :return:
        """
        already_namespaced_nodes = []
        new_nodes = []
        new_name_map = {}
        # First pass we validate + collect names so we can alter dependencies
        for node_ in nodes:
            new_name = assign_namespace(node_.name, namespace)
            new_name_map[node_.name] = new_name
            current_node_namespaces = node_.namespace
            if current_node_namespaces:
                already_namespaced_nodes.append(node_)
        for dep, value in self.inputs.items():
            # We create nodes for both namespace assignment and source assignment
            # Why? Cause we need unique parameter names, and with source() some can share params
            new_name_map[dep] = assign_namespace(dep, namespace)

        for dep, value in self.config.items():
            new_name_map[dep] = assign_namespace(dep, namespace)

        if already_namespaced_nodes:
            raise ValueError(
                f"The following nodes are already namespaced: {already_namespaced_nodes}. "
                f"We currently do not allow for multiple namespaces (E.G. layered subDAGs)."
            )
        # Reassign sources
        for node_ in nodes:
            new_name = new_name_map[node_.name]
            kwarg_mapping = {
                (new_name_map[key] if key in new_name_map else key): key
                for key in node_.input_types
            }

            # Map of argument in function to source, can't be the other way
            # around as sources can potentially serve multiple destinations (with the source()) decorator
            def fn(
                _callabl=node_.callable,
                _kwarg_mapping=dict(kwarg_mapping),
                _new_name=new_name,
                _new_name_map=dict(new_name_map),
                **kwargs,
            ):
                new_kwargs = {_kwarg_mapping[kwarg]: value for kwarg, value in kwargs.items()}
                return _callabl(**new_kwargs)

            new_input_types = {
                dep: node_.input_types[original_dep] for dep, original_dep in kwarg_mapping.items()
            }

            new_nodes.append(
                node_.copy_with(input_types=new_input_types, name=new_name, callabl=fn)
            )
        return new_nodes

    def add_final_node(self, fn: Callable, namespace: str):
        """

        :param fn:
        :return:
        """
        node_ = node.Node.from_fn(fn)
        namespaced_input_map = {assign_namespace(key, namespace): key for key in node_.input_types}
        new_input_types = {
            assign_namespace(key, namespace): value for key, value in node_.input_types.items()
        }

        def new_function(**kwargs):
            kwargs_without_namespace = {
                namespaced_input_map[key]: value for key, value in kwargs.items()
            }
            # Have to translate it back to use the kwargs the fn is expecting
            return fn(**kwargs_without_namespace)

        return node_.copy_with(input_types=new_input_types, callabl=new_function)

    def _derive_namespace(self, fn: Callable) -> str:
        """Utility function to derive a namespace from a function.
        The user will be able to likely pass this in as an override, but
        we have not exposed it yet.

        :param fn: Function we're decorating.
        :return: The function we're outputting.
        """
        return fn.__name__

    # def _derive_outputs_from_function(self, fn: Callable, nodes_produced: nodes) -> :

    def generate_nodes(self, fn: Callable, configuration: Dict[str, Any]) -> Collection[node.Node]:
        # Resolve all nodes from passed in functions
        nodes = self._collect_nodes(original_config=configuration)
        # Derive the namespace under which all these nodes will live
        namespace = self._derive_namespace(fn)
        # Rename them all to have the right namespace
        nodes = self._add_namespace(nodes, namespace)
        # Create any static input nodes we need to translate
        nodes += self._create_additional_static_nodes(nodes, namespace)
        # Add the final node that does the translation
        nodes += [self.add_final_node(fn, namespace)]
        return nodes

    def _validate_parameterization(self):
        invalid_values = []
        for key, value in self.inputs.items():
            if not isinstance(value, dependencies.ParametrizedDependency):
                invalid_values.append(value)
        if invalid_values:
            raise ValueError(
                f"Parameterization using the following values is not permitted -- "
                f"must be either source() or value(): {invalid_values}"
            )

    def validate(self, fn):
        """Validates everything we can before the

        :param fn: Function that this decorates
        :raises InvalidDecoratorException: if this is not a valid decorator
        """

        self._validate_parameterization()
