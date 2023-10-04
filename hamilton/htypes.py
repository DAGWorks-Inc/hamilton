import inspect
import sys
import typing
from abc import ABC
from typing import TYPE_CHECKING, Any, Generator, Tuple, Type, TypeVar

import typing_inspect

from hamilton.registry import COLUMN_TYPE, DF_TYPE_AND_COLUMN_TYPES

if TYPE_CHECKING:
    from hamilton.base import HamiltonGraphAdapter
BASE_ARGS_FOR_GENERICS = (typing.T,)


def _safe_subclass(candidate_type: Type, base_type: Type) -> bool:
    """Safely checks subclass, returning False if python's subclass does not work.
    This is *not* a true subclass check, and will not tell you whether hamilton
    considers the types to be equivalent. Rather, it is used to short-circuit further
    computation safely and avoid errors.

    Note that we may end up with types that *should* be considered equivalent, but
    are not. In that case we will deal with them -- its a better user experience and easier
    to report than an error.

    :param base_type: Base type to check against
    :param candidate_type: Candidate type to check as a potential subclass
    :return: Whether python considers them subclasses and will not break if subclass is called.
    """
    if len(_get_args(candidate_type)) > 0 or len(_get_args(base_type)) > 0:
        return False
    if inspect.isclass(candidate_type) and inspect.isclass(base_type):
        return issubclass(candidate_type, base_type)
    return False


def custom_subclass_check(requested_type: Type, param_type: Type):
    """This is a custom check around generics & classes. It probably misses a few edge cases.

    We will likely need to revisit this in the future (perhaps integrate with graphadapter?)

    :param requested_type: Candidate subclass.
    :param param_type: Type of parameter to check against.
    :return: Whether or not requested_type is a valid subclass of param_type.
    """
    # handles case when someone is using primitives and generics
    requested_origin_type = requested_type
    param_type, _ = get_type_information(param_type)
    param_origin_type = param_type
    has_generic = False
    if param_type == Any:
        # any type is a valid subclass of Any.
        return True
    if _safe_subclass(requested_type, param_type):
        return True
    if typing_inspect.is_union_type(param_type):
        for arg in _get_args(param_type):
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
    # TODO -- consider moving into a graph adapter or elsewhere -- this is perhaps a little too
    #  low-level
    if has_generic and requested_origin_type in (Parallelizable,):
        (requested_type_arg,) = _get_args(requested_type)
        return custom_subclass_check(requested_type_arg, param_type)
    if has_generic and param_origin_type == Collect:
        (param_type_arg,) = _get_args(param_type)
        return custom_subclass_check(requested_type, param_type_arg)
    if requested_origin_type == param_origin_type or _safe_subclass(
        requested_origin_type, param_origin_type
    ):
        if has_generic:  # check the args match or they do not have them defined.
            requested_args = _get_args(requested_type)
            param_args = _get_args(param_type)
            if (
                requested_args
                and param_args
                and requested_args != BASE_ARGS_FOR_GENERICS
                and param_args != BASE_ARGS_FOR_GENERICS
            ):
                return requested_args == param_args
        return True
    return False


def types_match(
    adapter: "HamiltonGraphAdapter", param_type: Type[Type], required_node_type: Any
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


_sys_version_info = sys.version_info
_version_tuple = (_sys_version_info.major, _sys_version_info.minor, _sys_version_info.micro)

"""
The following is purely for backwards compatibility
The behavior of annotated/get_args/get_origin has changed in recent versions
So we have to handle it accordingly
In 3.8/below we have to use the typing_extensions version

Also, note that it is currently called `column`, but
we will eventually want more options. E.G.

`dataset`
`scalar`
`tensor`

etc...

To do this, we'll likely extend from annotated, and add new types.
See `annotated` source code: https://github.com/python/cpython/blob/3.11/Lib/typing.py#L2122.

We can also potentially add validation in the types, and remove it from the validate.
"""

ANNOTATE_ALLOWED = False
if _version_tuple < (3, 9, 0):
    # Before 3.9 we use typing_extensions
    import typing_extensions

    column = typing_extensions.Annotated


else:
    ANNOTATE_ALLOWED = True
    from typing import Annotated, Type

    column = Annotated

if _version_tuple < (3, 9, 0):
    import typing_extensions

    _get_origin = typing_extensions.get_origin
    _get_args = typing_extensions.get_args
else:
    from typing import get_args as _get_args
    from typing import get_origin as _get_origin


def _is_annotated_type(type_: Type[Type]) -> bool:
    """Utility function to tell if a type is Annotated"""
    return _get_origin(type_) == column


# Placeholder exception for invalid hamilton types
class InvalidTypeException(Exception):
    pass


# Some valid series annotations
# We will likely have to expand
_valid_series_annotations = (
    int,
    float,
    str,
    bool,
)


def _is_valid_series_type(candidate_type: Type[Type]) -> bool:
    """Tells if something is a valid series type, using the registry we have.

    :param candidate_type: Type to check
    :return: Whether it is a series (column) type that we have registered
    """
    for key, types in DF_TYPE_AND_COLUMN_TYPES.items():
        if issubclass(candidate_type, types[COLUMN_TYPE]):
            return True
    return False


def validate_type_annotation(annotation: Type[Type]):
    """Validates a type annotation for a hamilton function.
    If it is not an Annotated type, it will be fine.
    If it is the Annotated type, it will check that
    it only has one type annotation and that that is valid (currently int, float, str, bool).

    :param annotation: Annotation (e.g. Annotated[pd.Series, int])
    :raises InvalidTypeException: If the annotation is invalid
    """

    if not _is_annotated_type(annotation):
        # In this case we don't care too much -- hamilton accepts anything
        return True
    original, *annotations = _get_args(annotation)
    # TODO -- use extensions/series types to do this more effectively
    if not (_is_valid_series_type(original)):
        raise InvalidTypeException(
            f"Hamilton only accepts annotated types of series or equivalent. Got {original}"
        )
    if len(annotations) > 1 or len(annotations) == 0:
        raise InvalidTypeException(
            f"Hamilton only accepts one annotation per pd.Series. Got {annotations}"
        )
    subclasses_valid_annotation = False
    (annotation,) = annotations
    for valid_annotation in _valid_series_annotations:
        if custom_subclass_check(annotation, valid_annotation):
            subclasses_valid_annotation = True
    if not subclasses_valid_annotation:
        raise InvalidTypeException(
            f"Hamilton only accepts annotations on series that are subclasses of one of {_valid_series_annotations}. "
            f"Got {annotation}"
        )


def get_type_information(some_type: Any) -> Tuple[Type[Type], list]:
    """Gets the type information for a given type.

    If it is an annotated type, it will return the original type and the annotation.
    If it is not an annotated type, it will return the type and empty list.

    :param some_type: Type to get information for
    :return: Tuple of type and list of annotations (or empty list)
    """
    if _is_annotated_type(some_type):
        original, *annotations = _get_args(some_type)
        return original, annotations
    return some_type, []


# Type variables for annotations below
T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


# TODO -- support sequential operation
# class Sequential(Generator[T, None, None], ABC):
#     pass


class Parallelizable(typing.Generator[U, None, None], ABC):
    pass


class Collect(Generator[V, None, None], ABC):
    pass


if __name__ == "__main__":
    print(get_type_information(column[list, int]))
    print(get_type_information(column[list, int, float]))
    print(get_type_information(float))
