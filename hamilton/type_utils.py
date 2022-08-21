import inspect
import typing
from typing import Any, Type

import typing_inspect

from hamilton import base

BASE_ARGS_FOR_GENERICS = (typing.T,)


def custom_subclass_check(requested_type: Type[Type], param_type: Type[Type]):
    """This is a custom check around generics & classes. It probably misses a few edge cases.

    We will likely need to revisit this in the future (perhaps integrate with graphadapter?)

    :param requested_type: Candidate subclass
    :param param_type: Type of parameter to check
    :return: Whether or not this is a valid subclass.
    """
    # handles case when someone is using primitives and generics
    requested_origin_type = requested_type
    param_origin_type = param_type
    has_generic = False
    if typing_inspect.is_union_type(param_type):
        for arg in typing_inspect.get_args(param_type):
            if custom_subclass_check(requested_type, arg):
                return True
    if typing_inspect.is_generic_type(requested_type) or typing_inspect.is_tuple_type(
        requested_type
    ):
        requested_origin_type = typing_inspect.get_origin(requested_type)
        has_generic = True
    if typing_inspect.is_generic_type(param_type) or typing_inspect.is_tuple_type(param_type):
        param_origin_type = typing_inspect.get_origin(param_type)
        has_generic = True
    if requested_origin_type == param_origin_type:
        if has_generic:  # check the args match or they do not have them defined.
            requested_args = typing_inspect.get_args(requested_type)
            param_args = typing_inspect.get_args(param_type)
            if (
                requested_args
                and param_args
                and requested_args != BASE_ARGS_FOR_GENERICS
                and param_args != BASE_ARGS_FOR_GENERICS
            ):
                return requested_args == param_args
        return True

    if (
        typing_inspect.is_generic_type(requested_type)
        and typing_inspect.is_generic_type(param_type)
    ) or (inspect.isclass(requested_type) and typing_inspect.is_generic_type(param_type)):
        # we're comparing two generics that aren't equal -- check if Mapping vs Dict
        # or we're comparing a class to a generic -- check if Mapping vs dict
        # the precedence is that requested will go into the param_type, so the param_type should be more permissive.
        return issubclass(requested_type, param_type)
    # classes - precedence is that requested will go into the param_type, so the param_type should be more permissive.
    if (
        inspect.isclass(requested_type)
        and inspect.isclass(param_type)
        and issubclass(requested_type, param_type)
    ):
        return True
    return False


def types_match(
    adapter: base.HamiltonGraphAdapter, param_type: Type[Type], required_node_type: Any
) -> bool:
    """Checks that we have "types" that "match".

    Matching can be loose here -- and depends on the adapter being used as to what is
    allowed. Otherwise it does a basic equality check.

    :param adapter: the graph adapter to delegate to for one check.
    :param param_type: the parameter type we're checking.
    :param required_node_type: the expected parameter type to validate against.
    :return: True if types are "matching", False otherwise.
    """
    if required_node_type == typing.Any:
        return True
    # type var  -- straight == should suffice. Assume people understand what they're doing with TypeVar.
    elif typing_inspect.is_typevar(required_node_type) or typing_inspect.is_typevar(param_type):
        return required_node_type == param_type
    elif required_node_type == param_type:
        return True
    elif custom_subclass_check(required_node_type, param_type):
        return True
    elif adapter.check_node_type_equivalence(required_node_type, param_type):
        return True
    return False
