"""Base lifecycle hooks/methods. This is *not* a public facing API -- see api.py for classes to extend.
This contains two sets of components:

1. Hooks/methods -- these are classes that customizes Hamilton's execution. They are called at specific
points in Hamilton's execution, and can be used to customize performance. There are specific rules about hooks
and methods
    - Methods can not (currently) be layered. This is because they replace a component of Hamilton's execution
    - Hooks can be layered. Multiple of the same hooks can be called at any given point.
2. Auxiliary tooling to register/manage hooks
    - LifecycleAdapterSet -- this is a class that manages a set of lifecycle adapters. It allows us to call
    all the lifecycle hooks/methods in a given set, and to determine if a given hook/method is implemented.
    - lifecycle -- this is a decorator container that allows us to register hooks/methods. It is used as follows:

To implement a new method/hook type:
1. Create a class that has a single method (see below for examples)
2. Decorate the class with the lifecycle decorator, passing in the name of the method/hook. This must correspond to a method on the class.
3. Add to the LifecycleAdapter type
4. Call out to the hook at different points in the lifecycle

Note that you can have one async hook/method and one sync hook/method in the same class. Some hooks/methods
are coupled to certain execution contexts. While they all live here for now, we could easily envision moving them
externally.

To build an implementation of a hook/method, all you have to do is extend any number of classes.
See api.py for implementations.
"""
import abc
import asyncio
import collections
import inspect
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Union

from hamilton import node

if TYPE_CHECKING:
    from hamilton.graph import FunctionGraph

from hamilton.node import Node

REGISTERED_SYNC_HOOKS: Set[str] = set()
REGISTERED_ASYNC_HOOKS: Set[str] = set()
REGISTERED_SYNC_METHODS: Set[str] = set()
REGISTERED_ASYNC_METHODS: Set[str] = set()

SYNC_HOOK = "hooks"
ASYNC_HOOK = "async_hooks"
SYNC_METHOD = "methods"
ASYNC_METHOD = "async_methods"


# Method to make registering hooks easy -- this is just in the superclass,
# and in the subclass it'll be automatically handled
# Note that it might be simpler to place the abstract abc.abstractmethod
# inside the decorator, but that would break IDEs/make development tougher.


class InvalidLifecycleAdapter(Exception):
    """Container exception to indicate that a lifecycle adapter is invalid."""

    pass


def validate_lifecycle_adapter_function(fn: Callable, returns_value: bool):
    """Validates that a function has arguments that are keyword-only,
    and either does or does not return a value, depending on the value of returns_value.

    :param fn: The function to validate
    :param returns_value: Whether the function should return a value or not
    """
    sig = inspect.signature(fn)
    if returns_value and sig.return_annotation is inspect.Signature.empty:
        raise InvalidLifecycleAdapter(
            f"Lifecycle methods must return a value, but {fn} does not have a return annotation."
        )
    if not returns_value and sig.return_annotation is not inspect.Signature.empty:
        raise InvalidLifecycleAdapter(
            f"Lifecycle hooks must not return a value, but {fn} has a return annotation."
        )
    for param in sig.parameters.values():
        if param.kind != inspect.Parameter.KEYWORD_ONLY and param.name != "self":
            raise InvalidLifecycleAdapter(
                f"Lifecycle methods/hooks can only have keyword-only arguments. "
                f"Method/hook {fn} has argument {param} that is not keyword-only."
            )


def validate_hook_fn(fn: Callable):
    """Validates that a function forms a valid hook. This means:
    1. Function returns nothing
    2. Function must consist of only kwarg-only arguments

    :param fn: The function to validate
    :raises InvalidLifecycleAdapter: If the function is not a valid hook
    """
    validate_lifecycle_adapter_function(fn, returns_value=False)


def validate_method_fn(fn: Callable):
    """Validates that a function forms a valid method. This means:
    1. Function returns a value
    2. Functions must consist of only kwarg-only arguments

    :param fn: The function to validate
    :raises InvalidLifecycleAdapter: If the function is not a valid method
    """
    validate_lifecycle_adapter_function(fn, returns_value=True)


class lifecycle:
    """Container class for decorators to register hooks/methods."""

    @classmethod
    def base_hook(cls, fn_name: str):
        """Hooks get called at distinct stages of Hamilton's execution.
        These can be layered together, and potentially coupled to other hooks."""

        def decorator(clazz):
            fn = getattr(clazz, fn_name, None)
            if fn is None:
                raise ValueError(
                    f"Class {clazz} does not have a method {fn_name}, but is "
                    f'decorated with @lifecycle.base_hook("{fn_name}"). The parameter '
                    f"to @lifecycle.base_hook must be the name "
                    f"of a method on the class."
                )
            validate_hook_fn(fn)
            if inspect.iscoroutinefunction(fn):
                setattr(clazz, ASYNC_HOOK, fn_name)
                REGISTERED_ASYNC_HOOKS.add(fn_name)
            else:
                setattr(clazz, SYNC_HOOK, fn_name)
                REGISTERED_SYNC_HOOKS.add(fn_name)
            return clazz

        return decorator

    @classmethod
    def base_method(cls, fn_name: str):
        """methods replace the default behavior of Hamilton at a given stage.
        Thus they can only be called once, and not layered. TODO -- determine
        how to allow multiple/have precedence for custom behavior."""

        def decorator(clazz):
            fn = getattr(clazz, fn_name, None)
            if fn is None:
                raise ValueError(
                    f"Class {clazz} does not have a method {fn_name}, but is "
                    f'decorated with @lifecycle.base_hook("{fn_name}"). The parameter '
                    f"to @lifecycle.base_hook must be the name "
                    f"of a method on the class."
                )
            validate_method_fn(fn)
            if inspect.iscoroutinefunction(fn):
                setattr(clazz, ASYNC_METHOD, fn_name)
                REGISTERED_ASYNC_METHODS.add(fn_name)
            else:
                setattr(clazz, SYNC_METHOD, fn_name)
                REGISTERED_SYNC_METHODS.add(fn_name)
            return clazz

        return decorator


@lifecycle.base_hook("pre_do_anything")
class BasePreDoAnythingHook(abc.ABC):
    @abc.abstractmethod
    def pre_do_anything(self):
        """Synchronous hook that gets called before doing anything, in the constructor of the driver."""
        pass


@lifecycle.base_method("do_check_edge_types_match")
class BaseDoCheckEdgeTypesMatch(abc.ABC):
    @abc.abstractmethod
    def do_check_edge_types_match(self, *, type_from: type, type_to: type) -> bool:
        """Method that checks whether two types are equivalent. This is used when the function graph is being created.

        :param type_from: The type of the node that is the source of the edge.
        :param type_to: The type of the node that is the destination of the edge.
        :return bool: Whether or not they are equivalent
        """
        pass


@lifecycle.base_method("do_validate_input")
class BaseDoValidateInput(abc.ABC):
    @abc.abstractmethod
    def do_validate_input(self, *, node_type: type, input_value: Any) -> bool:
        """Method that an input value maches an expected type.

        :param node_type:  The type of the node.
        :param input_value:  The value that we want to validate.
        :return: Whether or not the input value matches the expected type.
        """
        pass


@lifecycle.base_method("do_validate_node")
class BaseDoValidateNode(abc.ABC):
    @abc.abstractmethod
    def do_validate_node(self, *, created_node: node.Node) -> bool:
        """Validates a node. Note this is *not* integrated yet, so adding this in will be a No-op.
        In fact, we will likely be changing the API for this to have an optional error message.
        This is OK, as this is internal facing.

        Furthermore, we'll be adding in a user-facing API that takes in the tags, name, module, etc...

        :param created_node: Node that was created.
        :return: Whether or not the node is valid.
        """
        pass


@lifecycle.base_hook("post_graph_construct")
class BasePostGraphConstruct(abc.ABC):
    @abc.abstractmethod
    def post_graph_construct(
        self, *, graph: "FunctionGraph", modules: List[ModuleType], config: Dict[str, Any]
    ):
        """Hooks that is called after the graph is constructed.

        :param graph: Graph that has been constructed.
        :param modules: Modules passed into the graph
        :param config: Config passed into the graph
        """
        pass


@lifecycle.base_hook("post_graph_construct")
class BasePostGraphConstructAsync(abc.ABC):
    @abc.abstractmethod
    async def post_graph_construct(
        self, *, graph: "FunctionGraph", modules: List[ModuleType], config: Dict[str, Any]
    ):
        """Asynchronous hook that is called after the graph is constructed.

        :param graph: Graph that has been constructed.
        :param modules: Modules passed into the graph
        :param config: Config passed into the graph
        """
        pass


@lifecycle.base_hook("pre_graph_execute")
class BasePreGraphExecute(abc.ABC):
    @abc.abstractmethod
    def pre_graph_execute(
        self,
        *,
        run_id: str,
        graph: "FunctionGraph",
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
    ):
        """Hook that is called immediately prior to graph execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param graph:  Graph that is being executed
        :param final_vars: Variables we are extracting from the graph
        :param inputs: Inputs to the graph
        :param overrides: Overrides to graph execution
        """
        pass


@lifecycle.base_hook("pre_graph_execute")
class BasePreGraphExecuteAsync(abc.ABC):
    @abc.abstractmethod
    async def pre_graph_execute(
        self,
        *,
        run_id: str,
        graph: "FunctionGraph",
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
    ):
        """Asynchronous Hook that is called immediately prior to graph execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param graph:  Graph that is being executed
        :param final_vars: Variables we are extracting from the graph
        :param inputs: Inputs to the graph
        :param overrides: Overrides to graph execution
        """
        pass


@lifecycle.base_hook("pre_task_execute")
class BasePreTaskExecute(abc.ABC):
    @abc.abstractmethod
    def pre_task_execute(
        self,
        *,
        run_id: str,
        task_id: str,
        nodes: List[node.Node],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
    ):
        """Hook that is called immediately prior to task execution. Note that this is only useful in dynamic
        execution, although we reserve the right to add this back into the standard hamilton execution pattern.

        :param run_id: ID of the run, unique in scope of the driver.
        :param task_id: ID of the task, unique in scope of the driver.
        :param nodes: Nodes that are being executed
        :param inputs: Inputs to the task
        :param overrides: Overrides to task execution
        """
        pass


@lifecycle.base_hook("pre_task_execute")
class BasePreTaskExecuteAsync(abc.ABC):
    @abc.abstractmethod
    async def pre_task_execute(
        self,
        *,
        run_id: str,
        task_id: str,
        nodes: List[node.Node],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
    ):
        """Hook that is called immediately prior to task execution. Note that this is only useful in dynamic
        execution, although we reserve the right to add this back into the standard hamilton execution pattern.

        :param run_id: ID of the run, unique in scope of the driver.
        :param task_id: ID of the task, unique in scope of the driver.
        :param nodes: Nodes that are being executed
        :param inputs: Inputs to the task
        :param overrides: Overrides to task execution
        """
        pass


@lifecycle.base_hook("pre_node_execute")
class BasePreNodeExecute(abc.ABC):
    @abc.abstractmethod
    def pre_node_execute(
        self, *, run_id: str, node_: Node, kwargs: Dict[str, Any], task_id: Optional[str] = None
    ):
        """Hook that is called immediately prior to node execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param node_: Node that is being executed
        :param kwargs: Keyword arguments that are being passed into the node
        :param task_id: ID of the task, defaults to None if not in a task setting
        """
        pass


@lifecycle.base_hook("pre_node_execute")
class BasePreNodeExecuteAsync(abc.ABC):
    @abc.abstractmethod
    async def pre_node_execute(
        self, *, run_id: str, node_: Node, kwargs: Dict[str, Any], task_id: Optional[str] = None
    ):
        """Asynchronous hook that is called immediately prior to node execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param node_: Node that is being executed
        :param kwargs: Keyword arguments that are being passed into the node
        :param task_id: ID of the task, defaults to None if not in a task setting
        """
        pass


@lifecycle.base_method("do_node_execute")
class BaseDoNodeExecute(abc.ABC):
    @abc.abstractmethod
    def do_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        task_id: Optional[str] = None,
    ) -> Any:
        """Method that is called to implement node execution. This can replace the execution of a node
        with something all together, augment it, or delegate it.

        :param run_id: ID of the run, unique in scope of the driver.
        :param node_: Node that is being executed
        :param kwargs: Keyword arguments that are being passed into the node
        :param task_id: ID of the task, defaults to None if not in a task setting
        """
        pass


@lifecycle.base_method("do_node_execute")
class BaseDoNodeExecuteAsync(abc.ABC):
    @abc.abstractmethod
    async def do_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        task_id: Optional[str] = None,
    ) -> Any:
        """Asynchronous method that is called to implement node execution. This can replace the execution of a node
        with something all together, augment it, or delegate it.

        :param run_id: ID of the run, unique in scope of the driver.
        :param node_: Node that is being executed
        :param kwargs: Keyword arguments that are being passed into the node
        :param task_id: ID of the task, defaults to None if not in a task setting
        """
        pass


@lifecycle.base_hook("post_node_execute")
class BasePostNodeExecute(abc.ABC):
    @abc.abstractmethod
    def post_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        success: bool,
        error: Optional[Exception],
        result: Optional[Any],
        task_id: Optional[str] = None,
    ):
        """Hook that is called immediately after node execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param node_: Node that is being executed
        :param kwargs: Keyword arguments that are being passed into the node
        :param success: Whether or not the node executed successfully
        :param error: The error that was raised, if any
        :param result: The result of the node execution, if no error was raised
        :param task_id: ID of the task, defaults to None if not in a task-based execution
        """
        pass


@lifecycle.base_hook("post_node_execute")
class BasePostNodeExecuteAsync(abc.ABC):
    @abc.abstractmethod
    async def post_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        success: bool,
        error: Optional[Exception],
        result: Any,
        task_id: Optional[str] = None,
    ):
        """Hook that is called immediately after node execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param node_: Node that is being executed
        :param kwargs: Keyword arguments that are being passed into the node
        :param success: Whether or not the node executed successfully
        :param error: The error that was raised, if any
        :param result: The result of the node execution, if no error was raised
        :param task_id: ID of the task, defaults to None if not in a task-based execution
        """
        pass


@lifecycle.base_hook("post_task_execute")
class BasePostTaskExecute(abc.ABC):
    @abc.abstractmethod
    def post_task_execute(
        self,
        *,
        run_id: str,
        task_id: str,
        nodes: List[node.Node],
        results: Optional[Dict[str, Any]],
        success: bool,
        error: Exception,
    ):
        """Hook called immediately after task execution. Note that this is only useful in dynamic
        execution, although we reserve the right to add this back into the standard hamilton execution pattern.

        :param run_id: ID of the run, unique in scope of the driver.
        :param task_id: ID of the task
        :param nodes: Nodes that were executed
        :param results: Results of the task
        :param success: Whether or not the task executed successfully
        :param error: The error that was raised, if any
        """
        pass


@lifecycle.base_hook("post_task_execute")
class BasePostTaskExecuteAsync(abc.ABC):
    @abc.abstractmethod
    async def post_task_execute(
        self,
        *,
        run_id: str,
        task_id: str,
        nodes: List[node.Node],
        results: Optional[Dict[str, Any]],
        success: bool,
        error: Exception,
    ):
        """Asynchronous Hook called immediately after task execution. Note that this is only useful in dynamic
        execution, although we reserve the right to add this back into the standard hamilton execution pattern.

        :param run_id: ID of the run, unique in scope of the driver.
        :param task_id: ID of the task
        :param nodes: Nodes that were executed
        :param results: Results of the task
        :param success: Whether or not the task executed successfully
        :param error: The error that was raised, if any
        """
        pass


@lifecycle.base_hook("post_graph_execute")
class BasePostGraphExecute(abc.ABC):
    @abc.abstractmethod
    def post_graph_execute(
        self,
        *,
        run_id: str,
        graph: "FunctionGraph",
        success: bool,
        error: Optional[Exception],
        results: Optional[Dict[str, Any]],
    ):
        """Hook called immediately after graph execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param graph: Graph that was executed
        :param success: Whether or not the graph executed successfully
        :param error: Error that was raised, if any
        :param results: Results of the graph execution
        """
        pass


@lifecycle.base_hook("post_graph_execute")
class BasePostGraphExecuteAsync(abc.ABC):
    @abc.abstractmethod
    async def post_graph_execute(
        self,
        *,
        run_id: str,
        graph: "FunctionGraph",
        success: bool,
        error: Optional[Exception],
        results: Optional[Dict[str, Any]],
    ):
        """Asynchronous Hook called immediately after graph execution.

        :param run_id: ID of the run, unique in scope of the driver.
        :param graph: Graph that was executed
        :param success: Whether or not the graph executed successfully
        :param error: Error that was raised, if any
        :param results: Results of the graph execution
        """
        pass


@lifecycle.base_method("do_build_result")
class BaseDoBuildResult(abc.ABC):
    @abc.abstractmethod
    def do_build_result(self, *, outputs: Any) -> Any:
        """Method that is called to build the result of the graph execution.

        :param outputs: Output of the node execution
        :return: The final result
        """
        pass


# This is the type of a lifecycle adapter -- these types utilize

LifecycleAdapter = Union[
    BasePreDoAnythingHook,
    BaseDoCheckEdgeTypesMatch,
    BaseDoValidateInput,
    BaseDoValidateNode,
    BasePostGraphConstruct,
    BasePostGraphConstructAsync,
    BasePreGraphExecute,
    BasePreGraphExecuteAsync,
    BasePreTaskExecute,
    BasePreTaskExecuteAsync,
    BasePreNodeExecute,
    BasePreNodeExecuteAsync,
    BaseDoNodeExecute,
    BaseDoNodeExecuteAsync,
    BasePostNodeExecute,
    BasePostNodeExecuteAsync,
    BasePostTaskExecute,
    BasePostTaskExecuteAsync,
    BasePostGraphExecute,
    BasePostGraphExecuteAsync,
    BaseDoBuildResult,
]


class LifecycleAdapterSet:
    """An internal class that groups together all the lifecycle adapters.
    This allows us to call methods through a delegation pattern, enabling us to add
    whatever callbacks, logging, error-handling, etc... we need globally. While this
    does increase the stack trace in an error, it should be pretty easy to figure out what'g going on.
    """

    def __init__(self, *adapters: LifecycleAdapter):
        """Initializes the adapter set.

        :param adapters: Adapters to group together
        """
        self._adapters = adapters
        self.sync_hooks, self.async_hooks = self._get_lifecycle_hooks()
        self.sync_methods, self.async_methods = self._get_lifecycle_methods()

    def _get_lifecycle_hooks(
        self,
    ) -> Tuple[Dict[str, List[LifecycleAdapter]], Dict[str, List[LifecycleAdapter]]]:
        sync_hooks = collections.defaultdict(set)
        async_hooks = collections.defaultdict(set)
        for adapter in self.adapters:
            for cls in inspect.getmro(adapter.__class__):
                sync_hook = getattr(cls, SYNC_HOOK, None)
                if sync_hook is not None:
                    sync_hooks[sync_hook].add(adapter)
                async_hook = getattr(cls, ASYNC_HOOK, None)
                if async_hook is not None:
                    async_hooks[async_hook].add(adapter)
        return (
            {hook: list(adapters) for hook, adapters in sync_hooks.items()},
            {hook: list(adapters) for hook, adapters in async_hooks.items()},
        )

    def _get_lifecycle_methods(
        self,
    ) -> Tuple[Dict[str, List[LifecycleAdapter]], Dict[str, List[LifecycleAdapter]]]:
        sync_methods = collections.defaultdict(set)
        async_methods = collections.defaultdict(set)
        for adapter in self.adapters:
            for cls in inspect.getmro(adapter.__class__):
                sync_method = getattr(cls, SYNC_METHOD, None)
                if sync_method is not None:
                    sync_methods[sync_method].add(adapter)
                async_method = getattr(cls, ASYNC_METHOD, None)
                if async_method is not None:
                    async_methods[async_method].add(adapter)
        multiple_implementations_sync = [
            method for method, adapters in sync_methods.items() if len(adapters) > 1
        ]
        multiple_implementations_async = [
            method for method, adapters in async_methods.items() if len(adapters) > 1
        ]
        if len(multiple_implementations_sync) > 0 or len(multiple_implementations_async) > 0:
            raise ValueError(
                f"Multiple adapters cannot (currently) implement the same lifecycle method. "
                f"Sync methods: {multiple_implementations_sync}. "
                f"Async methods: {multiple_implementations_async}"
            )
        return (
            {method: list(adapters) for method, adapters in sync_methods.items()},
            {method: list(adapters) for method, adapters in async_methods.items()},
        )

    def does_hook(self, hook_name: str, is_async: bool) -> bool:
        """Whether or not a hook is implemented by any of the adapters in this group.
        If this hook is not registered, this will raise a ValueError.

        :param hook_name: Name of the hook
        :param is_async: Whether you want the async version or not
        :return: True if this adapter set does this hook, False otherwise
        """
        if is_async and hook_name not in REGISTERED_ASYNC_HOOKS:
            raise ValueError(
                f"Hook {hook_name} is not registered as an asynchronous lifecycle hook. "
                f"Registered hooks are {REGISTERED_ASYNC_HOOKS}"
            )
        if not is_async and hook_name not in REGISTERED_SYNC_HOOKS:
            raise ValueError(
                f"Hook {hook_name} is not registered as a synchronous lifecycle hook. "
                f"Registered hooks are {REGISTERED_SYNC_HOOKS}"
            )
        if not is_async:
            return hook_name in self.sync_hooks
        return hook_name in self.async_hooks

    def does_method(self, method_name: str, is_async: bool) -> bool:
        """Whether or not a method is implemented by any of the adapters in this group.
        If this method is not registered, this will raise a ValueError.

        :param method_name: Name of the method
        :param is_async: Whether you want the async version or not
        :return: True if this adapter set does this method, False otherwise
        """
        if is_async and method_name not in REGISTERED_ASYNC_METHODS:
            raise ValueError(
                f"Method {method_name} is not registered as an asynchronous lifecycle method. "
                f"Registered methods are {REGISTERED_ASYNC_METHODS}"
            )
        if not is_async and method_name not in REGISTERED_SYNC_METHODS:
            raise ValueError(
                f"Method {method_name} is not registered as a synchronous lifecycle method. "
                f"Registered methods are {REGISTERED_SYNC_METHODS}"
            )
        if not is_async:
            return method_name in self.sync_methods
        return method_name in self.async_methods

    def call_all_lifecycle_hooks_sync(self, hook_name: str, **kwargs):
        """Calls all the lifecycle hooks in this group, by hook name (stage)

        :param hook_name: Name of the hooks to call
        :param kwargs: Keyword arguments to pass into the hook
        """
        for adapter in self.sync_hooks[hook_name]:
            getattr(adapter, hook_name)(**kwargs)

    async def call_all_lifecycle_hooks_async(self, hook_name: str, **kwargs):
        """Calls all the lifecycle hooks in this group, by hook name (stage).

        :param hook_name: Name of the hook
        :param kwargs: Keyword arguments to pass into the hook
        """
        futures = []
        for adapter in self.async_hooks[hook_name]:
            futures.append(getattr(adapter, hook_name)(**kwargs))
        await asyncio.gather(*futures)

    def call_lifecycle_method_sync(self, method_name: str, **kwargs) -> Any:
        """Calls a lifecycle method in this group, by method name.

        :param method_name: Name of the method
        :param kwargs: Keyword arguments to pass into the method
        :return: The result of the method
        """
        if method_name not in REGISTERED_SYNC_METHODS:
            raise ValueError(
                f"Method {method_name} is not registered as a synchronous lifecycle method. "
                f"Registered methods are {REGISTERED_SYNC_METHODS}"
            )
        if method_name not in self.sync_methods:
            raise ValueError(
                f"Method {method_name} is not implemented by any of the adapters in this group. "  # TODO _- improve the error message
                f"Registered methods are {self.sync_methods}"
            )
        (adapter,) = self.sync_methods[method_name]
        return getattr(adapter, method_name)(**kwargs)

    async def call_lifecycle_method_async(self, method_name: str, **kwargs):
        """Call a lifecycle method in this group, by method name, async

        :param method_name: Name of the method
        :param kwargs: Keyword arguments to pass into the method
        :return: The result of the method
        """
        if method_name not in REGISTERED_ASYNC_METHODS:
            raise ValueError(
                f"Method {method_name} is not registered as an asynchronous lifecycle method. "
                f"Registered methods are {REGISTERED_ASYNC_METHODS}"
            )
        (adapter,) = self.async_methods[method_name]
        return await getattr(adapter, method_name)(**kwargs)

    @property
    def adapters(self) -> List[LifecycleAdapter]:
        """Gives the adapters in this group

        :return: A list of adapters
        """
        return self._adapters
