import abc
from abc import ABC
from typing import Any, Dict, List, Optional, Type

from hamilton import node
from hamilton.lifecycle.base import (
    BaseDoBuildResult,
    BaseDoCheckEdgeTypesMatch,
    BaseDoNodeExecute,
    BaseDoValidateInput,
    BasePostNodeExecute,
    BasePreNodeExecute,
)

try:
    from typing import override
except ImportError:
    override = lambda x: x  # noqa E731


class ResultBuilder(BaseDoBuildResult, abc.ABC):
    """Abstract class for building results. All result builders should inherit from this class and implement the build_result function.
    Note that applicable_input_type and output_type are optional, but recommended, for backwards
    compatibility. They let us type-check this. They will default to Any, which means that they'll
    connect to anything."""

    @abc.abstractmethod
    def build_result(self, **outputs: Any) -> Any:
        """Given a set of outputs, build the result.

        :param outputs: the outputs from the execution of the graph.
        :return: the result of the execution of the graph.
        """
        pass

    @override
    def do_build_result(self, outputs: Dict[str, Any]) -> Any:
        """Implements the do_build_result method from the BaseDoBuildResult class.
        This is kept from the user as the public-facing API is build_result, allowing us to change the
        API/implementation of the internal set of hooks"""
        return self.build_result(**outputs)

    def input_types(self) -> List[Type[Type]]:
        """Gives the applicable types to this result builder.
        This is optional for backwards compatibility, but is recommended.

        :return: A list of types that this can apply to.
        """
        return [Any]

    def output_type(self) -> Type:
        """Returns the output type of this result builder
        :return: the type that this creates
        """
        return Any


class LegacyResultMixin(ResultBuilder, ABC):
    """Backwards compatible legacy result builder. This utilizes a static method as we used to do that,
    although often times they got confused. If you want a result builder, use ResultBuilder above instead.
    """

    @staticmethod
    def build_result(**outputs: Any) -> Any:
        """Given a set of outputs, build the result.

        :param outputs: the outputs from the execution of the graph.
        :return: the result of the execution of the graph.
        """
        pass


class GraphAdapter(
    BaseDoNodeExecute,
    LegacyResultMixin,
    BaseDoValidateInput,
    BaseDoCheckEdgeTypesMatch,
    abc.ABC,
):
    """This is an implementation of HamiltonGraphAdapter, which has now been
    implemented with lifecycle methods/hooks."""

    @staticmethod
    @abc.abstractmethod
    def check_input_type(node_type: Type, input_value: Any) -> bool:
        """Used to check whether the user inputs match what the execution strategy & functions can handle.

        Static purely for legacy reasons.

        :param node_type: The type of the node.
        :param input_value: An actual value that we want to inspect matches our expectation.
        :return: True if the input is valid, False otherwise.
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def check_node_type_equivalence(node_type: Type, input_type: Type) -> bool:
        """Used to check whether two types are equivalent.

        Static, purely for legacy reasons.

        This is used when the function graph is being created and we're statically type checking the annotations
        for compatibility.

        :param node_type: The type of the node.
        :param input_type: The type of the input that would flow into the node.
        :return: True if the types are equivalent, False otherwise.
        """
        pass

    @override
    def do_node_execute(
        self, run_id: str, node_: node.Node, kwargs: Dict[str, Any], task_id: Optional[str] = None
    ) -> Any:
        return self.execute_node(node_, kwargs)

    @override
    def do_validate_input(self, node_type: type, input_value: Any) -> bool:
        return self.check_input_type(node_type, input_value)

    @override
    def do_check_edge_types_match(self, type_from: type, type_to: type) -> bool:
        return self.check_node_type_equivalence(type_to, type_from)

    @abc.abstractmethod
    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        """Given a node that represents a hamilton function, execute it.
        Note, in some adapters this might just return some type of "future".

        :param node: the Hamilton Node
        :param kwargs: the kwargs required to exercise the node function.
        :return: the result of exercising the node.
        """
        pass


class NodeExecutionHook(BasePreNodeExecute, BasePostNodeExecute, abc.ABC):
    """Implement this to hook into the node execution lifecycle. You can call anything before and after the driver"""

    @abc.abstractmethod
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
        """Hook that is executed prior to node execution.

        :param node_name: Name of the node.
        :param node_tags: Tags of the node
        :param node_kwargs: Keyword arguments to pass to the node
        :param node_return_type: Return type of the node
        :param task_id: The ID of the task, none if not in a task-based environment
        :param future_kwargs: Additional keyword arguments -- this is kept for backwards compatibility
        """

    def pre_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        task_id: Optional[str] = None,
    ):
        """Wraps the before_execution method, providing a bridge to an external-facing API. Do not override this!"""
        self.run_before_node_execution(
            node_name=node_.name,
            node_tags=node_.tags,
            node_kwargs=kwargs,
            node_return_type=node_.type,
            task_id=task_id,
        )

    @abc.abstractmethod
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
        """Hook that is executed post node execution.

        :param node_name: Name of the node in question
        :param node_tags: Tags of the node
        :param node_kwargs: Keyword arguments passed to the node
        :param node_return_type: Return type of the node
        :param result: Output of the node, None if an error occurred
        :param error: Error that occurred, None if no error occurred
        :param success: Whether the node executed successfully
        :param task_id: The ID of the task, none if not in a task-based environment
        :param future_kwargs: Additional keyword arguments -- this is kept for backwards compatibility
        """

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
        """Wraps the after_execution method, providing a bridge to an external-facing API. Do not override this!"""
        self.run_after_node_execution(
            node_name=node_.name,
            node_tags=node_.tags,
            node_kwargs=kwargs,
            node_return_type=node_.type,
            result=result,
            error=error,
            task_id=task_id,
            success=success,
        )


class EdgeConnectionHook(BaseDoCheckEdgeTypesMatch, BaseDoValidateInput, abc.ABC):
    def do_check_edge_types_match(self, *, type_from: type, type_to: type) -> bool:
        """Wraps the check_edge_types_match method, providing a bridge to an external-facing API. Do not override this!"""
        return self.check_edge_types_match(type_from, type_to)

    @abc.abstractmethod
    def check_edge_types_match(self, type_from: type, type_to: type, **kwargs: Any) -> bool:
        """This is run to check if edge types match. Note that this is an OR functionality
        -- this is run after we do some default checks, so this can only be permissive.
        Reach out if you want to be more restrictive than the default checks.

        :param type_from: The type of the node that is the source of the edge.
        :param type_to: The type of the node that is the destination of the edge.
        :param kwargs: This is kept for future backwards compatibility.
        :return: Whether or not the two node types form a valid edge.
        """
        pass

    def do_validate_input(self, *, node_type: type, input_value: Any) -> bool:
        """Wraps the validate_input method, providing a bridge to an external-facing API. Do not override this!"""
        return self.validate_input(node_type=node_type, input_value=input_value)

    @abc.abstractmethod
    def validate_input(self, node_type: type, input_value: Any, **kwargs: Any) -> bool:
        """This is run to check if the input is valid for the node type. Note that this is an OR functionality
        -- this is run after we do some default checks, so this can only be permissive.
        Reach out if you want to be more restrictive than the default checks.

        :param node_type: Type of the node that is accepting the input.
        :param input_value: Value of the input
        :param kwargs: Keyword arguments -- this is kept for future backwards compatibility.
        :return: Whether or not the input is valid for the node type.
        """
        pass


class NodeExecutionMethod(BaseDoNodeExecute):
    """API for executing a node. This takes in tags, callable, node name, and kwargs, and is
    responsible for executing the node and returning the result. Note this is not (currently)
    able to be layered together, although we may add that soon.
    """

    def do_node_execute(
        self,
        *,
        run_id: str,
        node_: node.Node,
        kwargs: Dict[str, Any],
        task_id: Optional[str] = None,
    ) -> Any:
        return self.run_to_execute_node(
            node_name=node_.name,
            node_tags=node_.tags,
            node_callable=node_.callable,
            node_kwargs=kwargs,
            task_id=task_id,
        )

    @abc.abstractmethod
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
        """This method is responsible for executing the node and returning the result.

        :param node_name: Name of the node.
        :param node_tags: Tags of the node.
        :param node_callable: Callable of the node.
        :param node_kwargs: Keyword arguments to pass to the node.
        :param task_id: The ID of the task, none if not in a task-based environment
        :param future_kwargs: Additional keyword arguments -- this is kept for backwards compatibility
        :return: The result of the node execution -- up to you to return this.
        """
        pass
