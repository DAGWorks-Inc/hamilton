import inspect
import sys
from types import ModuleType
from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Type, Union

_sys_version_info = sys.version_info
_version_tuple = (_sys_version_info.major, _sys_version_info.minor, _sys_version_info.micro)

if _version_tuple < (3, 11, 0):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired


from typing import TypedDict

# Copied this over from function_graph
# TODO -- determine the best place to put this code
from hamilton import graph_utils, node
from hamilton.function_modifiers import base, dependencies
from hamilton.function_modifiers.base import InvalidDecoratorException, NodeTransformer
from hamilton.function_modifiers.dependencies import (
    LiteralDependency,
    ParametrizedDependency,
    UpstreamDependency,
)


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


def create_identity_node(
    from_: str, typ: Type[Type], name: str, namespace: Tuple[str, ...], tags: Dict[str, Any]
) -> node.Node:
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
        namespace=namespace,
        tags=tags,
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


def create_static_node(
    typ: Type, name: str, value: Any, namespace: Tuple[str, ...], tags: Dict[str, Any]
) -> node.Node:
    """Utility function to create a static node -- this helps us bridge nodes together.

    :param typ: Type of the node to create
    :param name: Name of the node to create
    :param value: Value that the node's function always returns
    :param namespace: Namespace of the node
    :return: The instantiated static node
    """

    def node_fn(_value=value):
        return _value

    return node.Node(
        name=name, typ=typ, callabl=node_fn, input_types={}, namespace=namespace, tags=tags
    )


def _validate_config_inputs(config: Dict[str, Any], inputs: Dict[str, Any]):
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
    for key, value in inputs.items():
        if not isinstance(value, (UpstreamDependency, LiteralDependency)):
            raise InvalidDecoratorException(
                f"Input {key} must be either an UpstreamDependency or a LiteralDependency ,"
                f" not {type(value)}."
            )


NON_FINAL_TAGS = {NodeTransformer.NON_FINAL_TAG: True}


class subdag(base.NodeCreator):
    """The `@subdag` decorator enables you to rerun components of your DAG with varying parameters.
    That is, it enables you to "chain" what you could express with a driver into a single DAG.

    That is, instead of using Hamilton within itself:

    .. code-block:: python

        def feature_engineering(source_path: str) -> pd.DataFrame:
            '''You could recursively use Hamilton within itself.'''
            dr = driver.Driver({}, feature_modules)
            df = dr.execute(["feature_df"], inputs={"path": source_path})
            return df

    You instead can use the `@subdag` decorator to do the same thing, with the added benefit of visibility into the\
    whole DAG:

    .. code-block:: python

        @subdag(
            feature_modules,
            inputs={"path": source("source_path")},
            config={}
        )
        def feature_engineering(feature_df: pd.DataFrame) -> pd.DataFrame:
            return feature_df


    Note that this is immensely powerful -- if we draw analogies from Hamilton to standard procedural programming \
    paradigms, we might have the following correspondence:

    - `config.when` + friends -- `if/else` statements
    - `parameterize`/`extract_columns` -- `for` loop
    - `does` -- effectively macros

    And so on. `@subdag` takes this one step further:

    - `@subdag` -- subroutine definition

    E.G. take a certain set of nodes, and run them with specified parameters.

    @subdag declares parameters that are outputs of its subdags. Note that, if you want to use outputs of other
    components of the DAG, you can use the `external_inputs` parameter to declare the parameters that do *not* come
    from the subDAG.

    Why might you want to use this? Let's take a look at some examples:

    1. You have a feature engineering pipeline that you want to run on multiple datasets. If its exactly the same, \
    this is perfect. If not, this works perfectly as well, you just have to utilize different functions in each or \
    the `config.when` + `config` parameter to rerun it.
    2. You want to train multiple models in the same DAG that share some logic (in features or training) -- this \
    allows you to reuse and continually add more.
    3. You want to combine multiple similar DAGs (e.g. one for each business line) into one so you can build a \
    cross-business line model.

    This basically bridges the gap between the flexibility of non-declarative pipelining frameworks with the \
    readability/maintainability of declarative ones.
    """

    def __init__(
        self,
        *load_from: Union[ModuleType, Callable],
        inputs: Dict[str, ParametrizedDependency] = None,
        config: Dict[str, Any] = None,
        namespace: str = None,
        final_node_name: str = None,
        external_inputs: List[str] = None,
    ):
        """Adds a subDAG to the main DAG.

        :param load_from: The functions that will be used to generate this subDAG.
        :param inputs: Parameterized dependencies to inject into all sources of this subDAG.
            This should *not* be an intermediate node in the subDAG.
        :param config: A configuration dictionary for *just* this subDAG. Note that this passed in
            value takes precedence over the DAG's config.
        :param namespace: Namespace with which to prefix nodes. This is optional -- if not included,
            this will default to the function name.
        :param final_node_name: Name of the final node in the subDAG. This is optional -- if not included,
            this will default to the function name.
        :param external_inputs: Parameters in the function that are not produced by the functions
            passed to the subdag. This is useful if you want to perform some logic with other inputs
            in the subdag's processing function. Note that this is currently required to
            differentiate and clarify the inputs to the subdag.

        """
        self.subdag_functions = subdag.collect_functions(load_from)
        self.inputs = inputs if inputs is not None else {}
        self.config = config if config is not None else {}
        self.external_inputs = external_inputs if external_inputs is not None else []
        _validate_config_inputs(self.config, self.inputs)
        self.namespace = namespace
        self.final_node_name = final_node_name

    @staticmethod
    def collect_functions(
        load_from: Union[Collection[ModuleType], Collection[Callable]],
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

    @staticmethod
    def collect_nodes(config: Dict[str, Any], subdag_functions: List[Callable]) -> List[node.Node]:
        nodes = []
        for fn in subdag_functions:
            for node_ in base.resolve_nodes(fn, config):
                nodes.append(node_.copy_with(tags={**node_.tags, **NON_FINAL_TAGS}))
        return nodes

    def _create_additional_static_nodes(
        self, nodes: Collection[node.Node], namespace: str
    ) -> Collection[node.Node]:
        # These already have the namespace on them
        # This allows us to inject values into the replayed subdag
        node_types = extract_all_known_types(nodes)
        out = []
        for key, value in self.inputs.items():
            # TODO -- fix type derivation. Currently we don't use the specified type as we don't
            #  really know what it should be...
            new_node_name = assign_namespace(key, namespace)
            if value.get_dependency_type() == dependencies.ParametrizedDependencySource.LITERAL:
                out.append(
                    create_static_node(
                        typ=derive_type(value),
                        name=key,
                        value=value.value,
                        namespace=(namespace,),
                        tags=NON_FINAL_TAGS,
                    )
                )
            elif value.get_dependency_type() == dependencies.ParametrizedDependencySource.UPSTREAM:
                if new_node_name not in node_types:
                    continue
                out.append(
                    create_identity_node(
                        from_=value.source,
                        typ=node_types[new_node_name],
                        name=key,
                        namespace=(namespace,),
                        tags=NON_FINAL_TAGS,
                    )
                )
        for key, value in self.config.items():
            out.append(
                create_static_node(
                    typ=type(value),
                    name=key,
                    value=value,
                    namespace=(namespace,),
                    tags=NON_FINAL_TAGS,
                )
            )
        return out

    @staticmethod
    def add_namespace(
        nodes: List[node.Node],
        namespace: str,
        inputs: Dict[str, Any] = None,
        config: Dict[str, Any] = None,
    ) -> List[node.Node]:
        """Utility function to add a namespace to nodes.

        :param nodes:
        :return:
        """
        inputs = inputs if inputs is not None else {}
        config = config if config is not None else {}
        new_nodes = []
        new_name_map = {}
        # First pass we validate + collect names so we can alter dependencies
        for node_ in nodes:
            new_name = assign_namespace(node_.name, namespace)
            new_name_map[node_.name] = new_name
        for dep, _value in inputs.items():
            # We create nodes for both namespace assignment and source assignment
            # Why? Cause we need unique parameter names, and with source() some can share params
            new_name_map[dep] = assign_namespace(dep, namespace)

        for dep, _value in config.items():
            new_name_map[dep] = assign_namespace(dep, namespace)

        # Reassign sources
        for node_ in nodes:
            # This is not perfect -- we might get strangeness if its dynamically generated
            # that said, it should work
            is_async = inspect.iscoroutinefunction(node_.callable)
            new_name = new_name_map[node_.name]
            kwarg_mapping = {
                (new_name_map[key] if key in new_name_map else key): key
                for key in node_.input_types
            }

            # Map of argument in function to source, can't be the other way
            # around as sources can potentially serve multiple destinations (with the source()) decorator
            def fn(
                _callabl=node_.callable,
                _kwarg_mapping=dict(kwarg_mapping),  # noqa: B006
                _new_name=new_name,
                _new_name_map=dict(new_name_map),  # noqa: B006
                **kwargs,
            ):
                new_kwargs = {_kwarg_mapping[kwarg]: value for kwarg, value in kwargs.items()}
                return _callabl(**new_kwargs)

            async def async_fn(
                _callabl=node_.callable,
                _kwarg_mapping=dict(kwarg_mapping),  # noqa: B006
                _new_name=new_name,
                _new_name_map=dict(new_name_map),  # noqa: B006
                **kwargs,
            ):
                new_kwargs = {_kwarg_mapping[kwarg]: value for kwarg, value in kwargs.items()}
                return await _callabl(**new_kwargs)

            new_input_types = {
                dep: node_.input_types[original_dep] for dep, original_dep in kwarg_mapping.items()
            }
            fn_to_use = async_fn if is_async else fn

            new_nodes.append(
                node_.copy_with(input_types=new_input_types, name=new_name, callabl=fn_to_use)
            )
        return new_nodes

    def add_final_node(self, fn: Callable, node_name: str, namespace: str):
        """

        :param fn:
        :return:
        """
        is_async = inspect.iscoroutinefunction(fn)  # determine if its async
        node_ = node.Node.from_fn(fn)
        namespaced_input_map = {
            (assign_namespace(key, namespace) if key not in self.external_inputs else key): key
            for key in node_.input_types
        }

        new_input_types = {
            (assign_namespace(key, namespace) if key not in self.external_inputs else key): value
            for key, value in node_.input_types.items()
        }

        def new_function(**kwargs):
            kwargs_without_namespace = {
                namespaced_input_map[key]: value for key, value in kwargs.items()
            }
            # Have to translate it back to use the kwargs the fn is expecting
            return fn(**kwargs_without_namespace)

        async def async_function(**kwargs):
            return await new_function(**kwargs)

        fn_to_use = async_function if is_async else new_function

        return node_.copy_with(name=node_name, input_types=new_input_types, callabl=fn_to_use)

    def _derive_namespace(self, fn: Callable) -> str:
        """Utility function to derive a namespace from a function.

        :param fn: Function we're decorating.
        :return: The function we're outputting.
        """
        return fn.__name__ if self.namespace is None else self.namespace

    def _derive_name(self, fn: Callable) -> str:
        """Utility function to derive a name from a function.
        The user will be able to likely pass this in as an override, but
        we have not exposed it yet.

        :param fn: Function we're decorating.
        :return: The function we're outputting.
        """
        return fn.__name__ if self.final_node_name is None else self.final_node_name

    def generate_nodes(self, fn: Callable, configuration: Dict[str, Any]) -> Collection[node.Node]:
        # Resolve all nodes from passed in functions
        resolved_config = dict(configuration, **self.config)
        nodes = self.collect_nodes(config=resolved_config, subdag_functions=self.subdag_functions)
        # Derive the namespace under which all these nodes will live
        namespace = self._derive_namespace(fn)
        final_node_name = self._derive_name(fn)
        # Rename them all to have the right namespace
        nodes = self.add_namespace(nodes, namespace, self.inputs, self.config)
        # Create any static input nodes we need to translate
        nodes += self._create_additional_static_nodes(nodes, namespace)
        # Add the final node that does the translation
        nodes += [self.add_final_node(fn, final_node_name, namespace)]
        return nodes

    def _validate_parameterization(self):
        invalid_values = []
        for _key, value in self.inputs.items():
            if not isinstance(value, dependencies.ParametrizedDependency):
                invalid_values.append(value)
        if invalid_values:
            raise ValueError(
                f"Parameterization using the following values is not permitted -- "
                f"must be either source() or value(): {invalid_values}"
            )

    def validate(self, fn):
        """Validates everything we can before we create the subdag.

        :param fn: Function that this decorates
        :raises InvalidDecoratorException: if this is not a valid decorator
        """

        self._validate_parameterization()

    def required_config(self) -> Optional[List[str]]:
        """Currently we do not filter for subdag as we do not *statically* know what configuration
        is required. This is because we need to parse the function so that we can figure it out,
        and that is not available at the time that we call required_config. We need to think about
        the best way to do this, but its likely that we'll want to allow required_config to consume
        the function itself, and pass it in when its called with that.

        That said, we don't have sufficient justification to do that yet, so we're just going to
        return None for now, meaning that it has access to all configuration variables.

        :return:
        """
        return None


class SubdagParams(TypedDict):
    inputs: NotRequired[Dict[str, ParametrizedDependency]]
    config: NotRequired[Dict[str, Any]]
    external_inputs: NotRequired[List[str]]


class parameterized_subdag(base.NodeCreator):
    """parameterized subdag is when you want to create multiple subdags at one time.
    Why might you want to do this?

    1. You have multiple data sets you want to run the same feature engineering pipeline on.
    2. You want to run some sort of optimization routine with a variety of results
    3. You want to run some sort of pipeline over slightly different configuration (E.G. region/business line)

    Note that this really is just syntactic sugar for creating multiple subdags, just as `@parameterize
    is syntactic sugar for creating multiple nodes from a function. That said, it is common that you
    won't know what you want until compile time (E.G. when you have the config available), so this
    decorator along with the `@resolve` decorator is a good way to make that feasible. Note that
    we are getting into *advanced* Hamilton here -- we don't recommend starting with this. In fact,
    we generally recommend repeating subdags multiple times if you don't have too many. That said,
    that can get cumbersome if you have a lot, so this decorator is a good way to help with that.

    Let's take a look at an example:

    .. code-block:: python

        @parameterized_subdag(
            feature_modules,
            from_datasource_1={"inputs" : {"data" : value("datasource_1.csv"}},
            from_datasource_2={"inputs" : {"data" : value("datasource_2.csv"}},
            from_datasource_3={
                "inputs" : {"data" : value("datasource_3.csv"},
                "config" : {"filter" : "only_even_client_ids"}
            }
        )
        def feature_engineering(feature_df: pd.DataFrame) -> pd.DataFrame:
            return feature_df

    This is (obviously) contrived, but what it does is create three subdags, each with a different
    data source. The third one also applies a configuration to that subdags. Note that we can also
    pass in inputs/config to the decorator itself, which will be applied to all subdags.

    This is effectively the same as the example above.

    .. code-block:: python

        @parameterized_subdag(
            feature_modules,
            inputs={"data" : value("datasource_1.csv")},
            from_datasource_1={},
            from_datasource_2={
                    "inputs" : {"data" : value("datasource_2.csv"}
            },
            from_datasource_3={
                    "inputs" : {"data" : value("datasource_3.csv"},
                    "config" : {"filter" : "only_even_client_ids"},
            }
        )

    Again, think about whether this feature is really the one you want -- often times, verbose,
    static DAGs are far more readable than very concise, highly parameterized DAGs.
    """

    def __init__(
        self,
        *load_from: Union[ModuleType, Callable],
        inputs: Dict[
            str, Union[dependencies.ParametrizedDependency, dependencies.LiteralDependency]
        ] = None,
        config: Dict[str, Any] = None,
        external_inputs: List[str] = None,
        **parameterization: SubdagParams,
    ):
        """Initializes a parameterized_subdag decorator.

        :param load_from: Modules to load from
        :param inputs: Inputs for each subdag generated by the decorated function
        :param config: Config for each subdag generated by the decorated function
        :param external_inputs: External inputs to all parameterized subdags. Note that
            if you pass in any external inputs from local subdags, it overrides this (does not merge).
        :param parameterization: Parameterizations for each subdag generated.
            Note that this *overrides* any inputs/config passed to the decorator itself.

            Furthermore, note the following:

            1. The parameterizations passed to the constructor are \\*\\*kwargs, so you are not
            allowed to name these `load_from`, `inputs`, or `config`. That's a good thing, as these
            are not good names for variables anyway.

            2. Any empty items (not included) will default to an empty dict (or an empty list in
            the case of parameterization)
        """
        self.load_from = load_from
        self.inputs = inputs if inputs is not None else {}
        self.config = config if config is not None else {}
        self.parameterization = parameterization
        self.external_inputs = external_inputs if external_inputs is not None else []

    def _gather_subdag_generators(self) -> List[subdag]:
        subdag_generators = []
        for key, parameterization in self.parameterization.items():
            subdag_generators.append(
                subdag(
                    *self.load_from,
                    inputs={**self.inputs, **parameterization.get("inputs", {})},
                    config={**self.config, **parameterization.get("config", {})},
                    external_inputs=parameterization.get("external_inputs", self.external_inputs),
                    namespace=key,
                    final_node_name=key,
                )
            )
        return subdag_generators

    def generate_nodes(self, fn: Callable, config: Dict[str, Any]) -> List[node.Node]:
        generated_nodes = []
        for subdag_generator in self._gather_subdag_generators():
            generated_nodes.extend(subdag_generator.generate_nodes(fn, config))
        return generated_nodes

    def validate(self, fn: Callable):
        for subdag_generator in self._gather_subdag_generators():
            subdag_generator.validate(fn)

    def required_config(self) -> Optional[List[str]]:
        """See note for subdag.required_config -- this is the same pattern.

        :return: Any required config items.
        """
        return None
