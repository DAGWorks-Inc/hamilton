from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from hamilton import htypes, node, registry
from hamilton.function_modifiers import base

"""Decorators that attach metadata to nodes"""


class tag(base.NodeDecorator):
    """Decorator class that adds a tag to a node. Tags take the form of key/value pairings.
    Tags can have dots to specify namespaces (keys with dots), but this is usually reserved for special cases
    (E.G. subdecorators) that utilize them. Usually one will pass in tags as kwargs, so we expect tags to
    be un-namespaced in most uses.

    That is using:

    .. code-block:: python

       @tag(my_tag='tag_value')
       def my_function(...) -> ...:

    is un-namespaced because you cannot put a `.` in the keyword part (the part before the '=').

    But using:

    .. code-block:: python

       @tag(**{'my.tag': 'tag_value'})
       def my_function(...) -> ...:

    allows you to add dots that allow you to namespace your tags.

    Currently, tag values are restricted to allowing strings only, although we may consider changing the in the future
    (E.G. thinking of lists).

    Hamilton also reserves the right to change the following:
    * adding purely positional arguments
    * not allowing users to use a certain set of top-level prefixes (E.G. any tag where the top level is one of the \
    values in RESERVED_TAG_PREFIX).

    Example usage:

    .. code-block:: python

       @tag(foo='bar', a_tag_key='a_tag_value', **{'namespace.tag_key': 'tag_value'})
       def my_function(...) -> ...:
          ...
    """

    RESERVED_TAG_NAMESPACES = [
        "hamilton",
        "data_quality",
        "gdpr",
        "ccpa",
        "dag",
        "module",
    ]  # Anything that starts with any of these is banned, the framework reserves the right to manage it

    def __init__(self, *, target_: base.TargetType = None, **tags: Union[str, List[str]]):
        """Constructor for adding tag annotations to a function.

        :param target\\_: Target nodes to decorate. This can be one of the following:

            * **None**: tag all nodes outputted by this that are "final" (E.g. do not have a node\
            outputted by this that depend on them)
            * **Ellipsis (...)**: tag *all* nodes outputted by this
            * **Collection[str]**: tag *only* the nodes with the specified names
            * **str**: tag *only* the node with the specified name
        :param tags: the keys are always going to be strings, so the type annotation here means the values are strings \
            or lists of values. Implicitly this is `Dict[str, Union[str, List[str]]]` but the PEP guideline is to only
             annotate it with the value `Union[str, List[str]]`.
        """
        super(tag, self).__init__(target=target_)
        self.tags = tags

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
        if not isinstance(value, str) and not isinstance(value, list):
            return False
        elif isinstance(value, list):
            if len(value) == 0:
                return False  # disallow empty lists
            if not all([isinstance(v, str) for v in value]):  # need values to be all strings
                return False
            if len(set(value)) != len(value):  # need values to be unique
                return False
        return True

    def validate(self, fn: Callable):
        """Validates the decorator. In this case that the set of tags produced is final.

        :param fn: Function that the decorator is called on.
        :raises ValueError: if the specified tags contains invalid ones
        """
        bad_tags = set()
        for key, value in self.tags.items():
            if (not tag._key_allowed(key)) or (not tag._value_allowed(value)):
                if isinstance(value, list):
                    value = str(value)
                bad_tags.add((key, value))
        if bad_tags:
            bad_tags_formatted = ",".join([f"{key}={value}" for key, value in bad_tags])
            raise base.InvalidDecoratorException(
                f"The following tags are invalid as tags: {bad_tags_formatted} "
                "Tag keys can be split by ., to represent a hierarchy, "
                "but each element of the hierarchy must be a valid python identifier. "
                "Paths components also cannot be empty. "
                "The value can only be a string, or a list of strings. "
                "Note that the following top-level prefixes are "
                f"reserved as well: {self.RESERVED_TAG_NAMESPACES}"
            )


class tag_outputs(base.NodeDecorator):
    def __init__(self, **tag_mapping: Dict[str, Union[str, List[str]]]):
        """Creates a tag_outputs decorator.

        Note that this currently does not validate whether the nodes are spelled correctly as it takes in a superset of\
        nodes.

        :param tag_mapping: Mapping of output name to tags -- this is akin to applying @tag to individual outputs \
        produced by the function.

        Example usage:

        .. code-block:: python

           @tag_output(**{'a': {'a_tag': 'a_tag_value'}, 'b': {'b_tag': 'b_tag_value'}})
           @extract_columns("a", "b")
           def example_tag_outputs() -> pd.DataFrame:
               return pd.DataFrame.from_records({"a": [1], "b": [2]})

        """
        super(base.NodeDecorator, self).__init__(target=...)
        self.tag_mapping = tag_mapping

    def decorate_node(self, node_: node.Node) -> node.Node:
        """Decorates all final nodes with the specified tags."""
        if node_.name not in self.tag_mapping:
            return node_  # in this case we have no desire to update tags
        new_tags = node_.tags.copy()
        new_tags.update(self.tag_mapping.get(node_.name, {}))
        return tag(**new_tags).decorate_node(node_)


# These represent a generic schema type -- E.G. one that will
# be supported across the entire set of usable dataframe/dataset types
# Eventually we'll be integrating mappings of these into the registry,
# but for now this serves largely as a placeholder/documentation
# GENERIC_SCHEMA_TYPES = (
#     "int",
#     "float",
#     "str",
#     "bool",
#     "dict",
#     "list",
#     "object",
#     "datetime",
#     "date",
# )


class SchemaOutput(tag):
    def __init__(self, *fields: Tuple[str, str], target_: Optional[str] = None):
        """Initializes SchemaOutput. See docs for `@schema.output` for more details."""

        tag_value = ",".join([f"{key}={value}" for key, value in fields])
        super(SchemaOutput, self).__init__(
            **{schema.INTERNAL_SCHEMA_OUTPUT_KEY: tag_value}, target_=target_
        )

    def validate_node(self, node_: node.Node):
        """Validates that the node has a return type of a registered dataframe.

        :param node_: Node to validate
        :raises InvalidDecoratorException: if the node does not have a return type of a registered dataframe.
        """
        output_type = node_.type
        available_types = registry.get_registered_dataframe_types()
        for _, type_ in available_types.items():
            if htypes.custom_subclass_check(output_type, type_):
                return
        raise base.InvalidDecoratorException(
            f"Node {node_.name} has type {output_type} which is not a registered type for a dataset. "
            f"Registered types are {available_types}. If you found this, either (a) ensure you have the "
            f"right package installed, or (b) reach out to the team to figure out how to add yours."
        )

    @classmethod
    def allows_multiple(cls) -> bool:
        """Currently this only applies to a single output. If it is a set of nodes with multiple outputs,
        it will apply to the "final" (sink) one. We can change this if there's need."""
        return False

    def validate(self, fn: Callable):
        """Bypassed for now -- we have no function-level or class-level validations yet,
        but this is done at `@tag`, which this inherits. We will be moving away from inheriting tag.
        """
        pass


class schema:
    """Container class for schema stuff. This is purely so we can have a nice API for it -- E.G. Schema.output"""

    INTERNAL_SCHEMA_OUTPUT_KEY = "hamilton.internal.schema_output"

    @staticmethod
    def output(*fields: Tuple[str, str], target_: Optional[str] = None) -> SchemaOutput:
        """Initializes a `@schema.output` decorator. This takes in a list of fields, which are tuples of the form
        `(field_name, field_type)`. The field type must be one of the function_modifiers.SchemaTypes types.

        :param target_: Target node to decorate -- if `None` it'll decorate all final nodes (E.G. sinks in the subdag),
            otherwise it will decorate the specified node.
        :param fields: List of fields to add to the schema. Each field is a tuple of the form `(field_name, field_type)`

        This is implemented using tags, but that might change. Thus you should not
        rely on the tags created by this decorator (which is why they are prefixed with `internal`).

        To use this, you should decorate a node with `@schema.output`

        Example usage:

        .. code-block:: python

           @schema.output(
               ("a", "int"),
               ("b", "float"),
               ("c", "str")
            )
           def example_schema() -> pd.DataFrame:
               return pd.DataFrame.from_records({"a": [1], "b": [2.0], "c": ["3"]})

        Then, when drawing the DAG, the schema will be displayed as sub-elements in the node for the DAG (if `display_schema` is selected).
        """
        return SchemaOutput(*fields, target_=target_)
