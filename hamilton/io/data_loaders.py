import abc
import dataclasses
import typing
from typing import Any, Dict, Tuple, Type, TypeVar

LoadType = TypeVar("LoadType")


class DataLoader(abc.ABC):
    """Base class for data loaders. Data loaders are used to load data from a data source.
    Note that they are inherently polymorphic -- they declare what type(s) they can load to,
    and may choose to load differently depending on the type they are loading to.

    Note that this is not yet a public-facing API -- the current set of data loaders will
    be managed by the library, and the user will not be able to create their own.

    We intend to change this and provide an extensible user-facing API,
    but if you subclass this, beware! It might change.
    """

    @classmethod
    @abc.abstractmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        """Tells whether or not this data loader can load to a specific type.
        For instance, a CSV data loader might be able to load to a dataframe,
        a json, but not an integer.

        This is a classmethod as it will be easier to validate, and we have to
        construct this, delayed, with a factory.

        :param type_: Candidate type
        :return: True if this data loader can load to the type, False otherwise.
        """
        pass

    @abc.abstractmethod
    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        """Loads the data from the data source.
        Note this uses the constructor parameters to determine
        how to load the data.

        :return: The type specified
        """
        pass

    @classmethod
    @abc.abstractmethod
    def name(cls) -> str:
        """Returns the name of the data loader. This is used to register the data loader
        with the load_from decorator.

        :return: The name of the data loader.
        """
        pass

    @classmethod
    def _ensure_dataclass(cls):
        if not dataclasses.is_dataclass(cls):
            raise TypeError(
                f"DataLoader subclasses must be dataclasses. {cls.__qualname__} is not."
                f" Did you forget to add @dataclass?"
            )

    @classmethod
    def get_required_arguments(cls) -> Dict[str, Type[Type]]:
        """Gives the required arguments for the class.
        Note that this just uses the type hints from the dataclass.

        :return: The required arguments for the class.
        """
        cls._ensure_dataclass()
        type_hints = typing.get_type_hints(cls)
        return {
            field.name: type_hints.get(field.name)
            for field in dataclasses.fields(cls)
            if field.default == dataclasses.MISSING
        }

    @classmethod
    def get_optional_arguments(cls) -> Dict[str, Tuple[Type[Type], Any]]:
        """Gives the optional arguments for the class.
        Note that this just uses the type hints from the dataclass.

        :return: The optional arguments for the class.
        """
        cls._ensure_dataclass()
        type_hints = typing.get_type_hints(cls)
        return {
            field.name: type_hints.get(field.name)
            for field in dataclasses.fields(cls)
            if field.default != dataclasses.MISSING
        }
