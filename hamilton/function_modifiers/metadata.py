import sys
import typing
from typing import Any, Callable, Dict, Type

if sys.version_info < (3, 8):
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

import typing_inspect

from hamilton import node
from hamilton.function_modifiers import base

"""Decorators that attach metadata to nodes"""


class tag(base.NodeDecorator):
    """Decorator class that adds a tag to a node. Tags take the form of key/value pairings.
    Tags can have dots to specify namespaces (keys with dots), but this is usually reserved for special cases
    (E.G. subdecorators) that utilize them. Usually one will pass in tags as kwargs, so we expect tags to
    be un-namespaced in most uses.

    That is using:
    > @tag(my_tag='tag_value')
    > def my_function(...) -> ...:
    is un-namespaced because you cannot put a `.` in the keyword part (the part before the '=').

    But using:
    > @tag(**{'my.tag': 'tag_value'})
    > def my_function(...) -> ...:
    allows you to add dots that allow you to namespace your tags.

    Currently, tag values are restricted to allowing strings only, although we may consider changing the in the future
    (E.G. thinking of lists).

    Hamilton also reserves the right to change the following:
    * adding purely positional arguments
    * not allowing users to use a certain set of top-level prefixes (E.G. any tag where the top level is one of the
      values in RESERVED_TAG_PREFIX).

    Example usage:
    > @tag(foo='bar', a_tag_key='a_tag_value', **{'namespace.tag_key': 'tag_value'})
    > def my_function(...) -> ...:
    >   ...
    """

    RESERVED_TAG_NAMESPACES = [
        "hamilton",
        "data_quality",
        "gdpr",
        "ccpa",
        "dag",
        "module",
    ]  # Anything that starts with any of these is banned, the framework reserves the right to manage it

    def __init__(self, *, __validate_tag_types: bool = True, **tags: str):
        """Constructor for adding tag annotations to a function.

        :param tags: the keys are always going to be strings, so the type annotation here means the values are strings.
            Implicitly this is `Dict[str, str]` but the PEP guideline is to only annotate it with `str`.
        :param __validate_tag_types: If true, we validate the types of the tags. This is called by the framework, and
            should not be called by users. If you want to have more than just str valued tags, consider using typed tags
            as specified below.
        """
        self.tags = tags
        self.__validate_tag_types = __validate_tag_types

    def decorate_node(self, node_: node.Node) -> node.Node:
        """Decorates the nodes produced by this with the specified tags

        :param node_: Node to decorate
        :return: Copy of the node, with tags assigned
        """
        node_tags = node_.tags.copy()
        node_tags.update(self.tags)
        return node_.copy_with(tags=node_tags)

    @staticmethod
    def _key_allowed(key: str) -> bool:
        """Validates that a tag key is allowed. Rules are:
        1. It must not be empty
        2. It can have dots, which specify a hierarchy of order
        3. All components, when split by dots, must be valid python identifiers
        4. It cannot utilize a reserved namespace

        :param key: The key to validate
        :return: True if it is valid, False if not
        """
        key_components = key.split(".")
        if len(key_components) == 0:
            # empty string...
            return False
        if key_components[0] in tag.RESERVED_TAG_NAMESPACES:
            # Reserved prefixes
            return False
        for key in key_components:
            if not key.isidentifier():
                return False
        return True

    @staticmethod
    def _value_allowed(value: Any) -> bool:
        """Validates that a tag value is allowed. Rules are only that it must be a string.

        :param value: Value to validate
        :return: True if it is valid, False otherwise
        """
        if not isinstance(value, str):
            return False
        return True

    def validate(self, fn: Callable):
        """Validates the decorator. In this case that the set of tags produced is final.

        :param fn: Function that the decorator is called on.
        :raises ValueError: if the specified tags contains invalid ones
        """
        bad_tags = set()
        for key, value in self.tags.items():
            if not tag._key_allowed(key):
                bad_tags.add((key, value))
            if not tag._value_allowed(value) and not self.__validate_tag_types:
                bad_tags.add((key, value))

        if bad_tags:
            bad_tags_formatted = ",".join([f"{key}={value}" for key, value in bad_tags])
            raise base.InvalidDecoratorException(
                f"The following tags are invalid as tags: {bad_tags_formatted} "
                "Tag keys can be split by ., to represent a hierarchy, "
                "but each element of the hierarchy must be a valid python identifier. "
                "Paths components also cannot be empty. "
                "The value can be anything. Note that the following top-level prefixes are "
                f"reserved as well: {self.RESERVED_TAG_NAMESPACES}"
            )


class tag_outputs(base.NodeDecorator):
    def __init__(self, **tag_mapping: Dict[str, str]):
        """Creates a tag_outputs decorator. Note that this currently does not validate whether the
        nodes are spelled correctly as it takes in a superset of nodes.
        :param tag_mapping: Mapping of node name to tags -- this is akin to applying @tag to individual nodes produced by the function
        """
        self.tag_mapping = tag_mapping

    def decorate_node(self, node_: node.Node) -> node.Node:
        """Decorates all final nodes with the specified tags."""
        new_tags = node_.tags.copy()
        new_tags.update(self.tag_mapping.get(node_.name, {}))
        return tag(**new_tags).decorate_node(node_)


# class TypedTagSet(TypedDict):
#     """A typed tag set is a dictionary of tags that are typed. We do additional validation on this
#     to ensure that the right types are created and that that ri"""


def _type_allowed(type: Type[Type], allow_lists: bool = True) -> bool:
    """Validates that a type is allowed. We only allow primitive types and lists of primitive types"""
    if type in [int, float, str, bool]:
        return True
    if allow_lists:
        if typing_inspect.is_generic_type(type):
            if typing_inspect.get_origin(type) == list:
                return _type_allowed(typing_inspect.get_args(type)[0], allow_lists=False)
    return False


def _validate_spec(typed_dict_class: Type[TypedDict]):
    invalid_types = []
    for key, value in typing.get_type_hints(typed_dict_class).items():
        if not _type_allowed(value, allow_lists=True):
            invalid_types.append((key, value))
    if invalid_types:
        invalid_types_formatted = ",".join([f"{key}={value}" for key, value in invalid_types])
        raise base.InvalidDecoratorException(
            f"The following key/value pairs are invalid as types: {invalid_types_formatted} "
            "Types can be any primitive type or a list of a primitive type."
        )


def _type_matches(value: Any, type_: Type[Type]):
    if type_ in [int, float, str, bool]:
        return isinstance(value, type_)
    if typing_inspect.is_generic_type(type_):
        if typing_inspect.get_origin(type_) == list:
            return isinstance(value, list) and all(
                _type_matches(item, typing_inspect.get_args(type_)[0]) for item in value
            )
    return False


def _validate_values(typed_dict: dict, typed_dict_class: Type[TypedDict]):
    invalid_pairs = []
    for key, value in typed_dict.items():
        if not _type_matches(value, typing.get_type_hints(typed_dict_class)[key]):
            invalid_pairs.append((key, value))
    if invalid_pairs:
        invalid_pairs_formatted = ",".join([f"{key}={value}" for key, value in invalid_pairs])
        raise base.InvalidDecoratorException(
            f"The following key/value pairs are invalid as values: {invalid_pairs_formatted} "
            "Values must match the specified type."
        )


def validate_typed_dict(data: dict, typed_dict_class: TypedDict):
    _validate_spec(typed_dict_class)
    _validate_values(data, typed_dict_class)


class typed_tags:
    def __init__(self, typed_tag_class: TypedDict):
        self.tag_set_type = typed_tag_class

    def __call__(self, **kwargs: Any):
        validate_typed_dict(dict(**kwargs), self.tag_set_type)
        return tag(**kwargs, __validate_tag_types=False)  # types are already validated
