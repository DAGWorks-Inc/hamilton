import collections
import functools
import importlib
import logging
from typing import Any, Dict, Optional, Type

logger = logging.getLogger(__name__)

# Use this to ensure the registry is loaded only once.
INITIALIZED = False

# This is a dictionary of extension name -> dict with dataframe and column types.
DF_TYPE_AND_COLUMN_TYPES: Dict[str, Dict[str, Type]] = {}

COLUMN_TYPE = "column_type"
DATAFRAME_TYPE = "dataframe_type"


def register_types(extension_name: str, dataframe_type: Type, column_type: Optional[Type]):
    """Registers the dataframe and column types for the extension. Note that column types are optional
    as some extensions may not have a column type (E.G. spark). In this case, this is not included

    :param extension_name: name of the extension doing the registering.
    :param dataframe_type: the dataframe type to register.
    :param column_type: the column type to register
    """
    global DF_TYPE_AND_COLUMN_TYPES
    output = {}
    output[DATAFRAME_TYPE] = dataframe_type
    if column_type is not None:
        output[COLUMN_TYPE] = column_type
    DF_TYPE_AND_COLUMN_TYPES[extension_name] = output


@functools.singledispatch
def get_column(df: Any, column_name: str):
    """Gets a column from a dataframe.

    Each extension should register a function for this.

    :param df: the dataframe.
    :param column_name: the column name.
    :return: the correct "representation" of a column for this "dataframe".
    """
    raise NotImplementedError()


@functools.singledispatch
def fill_with_scalar(df: Any, column_name: str, scalar_value: Any) -> Any:
    """Fills a column with a scalar value.

    :param df: the dataframe.
    :param column_name: the column to fill.
    :param scalar_value: the scalar value to fill with.
    :return: the modified dataframe.
    """
    raise NotImplementedError()


def get_column_type_from_df_type(dataframe_type: Type) -> Type:
    """Function to cycle through the registered extensions and return the column type for the dataframe type.

    :param dataframe_type: the dataframe type to find the column type for.
    :return: the column type.
    :raises: NotImplementedError if we don't know what the column type is.
    """
    for extension, type_map in DF_TYPE_AND_COLUMN_TYPES.items():
        if dataframe_type == type_map[DATAFRAME_TYPE]:
            return type_map[COLUMN_TYPE]
    raise NotImplementedError(
        f"Cannot get column type for [{dataframe_type}]. "
        f"Registered types are {DF_TYPE_AND_COLUMN_TYPES}"
    )


def load_extension(plugin_module: str):
    """Given a module name, loads it for Hamilton to use.

    :param plugin_module: the module name sans .py. e.g. pandas, polars, pyspark_pandas.
    """
    mod = importlib.import_module(f"hamilton.plugins.{plugin_module}_extensions")
    # We have various plugin extensions. We default to assuming it's a dataframe extension with columns,
    # unless it explicitly says it's not.
    # We need to check the following if we are to enable `@extract_columns` for example.
    extractable = getattr(mod, "COLUMN_FRIENDLY_DF_TYPE", True)
    if extractable:
        assert hasattr(mod, "register_types"), "Error extension missing function register_types()"
        assert hasattr(
            mod, f"get_column_{plugin_module}"
        ), f"Error extension missing get_column_{plugin_module}"
        assert hasattr(
            mod, f"fill_with_scalar_{plugin_module}"
        ), f"Error extension missing fill_with_scalar_{plugin_module}"
        logger.info(f"Detected {plugin_module} and successfully loaded Hamilton extensions.")


LOADER_REGISTRY = collections.defaultdict(list)
SAVER_REGISTRY = collections.defaultdict(list)


def register_adapter(adapter: Any):
    """Registers a adapter. Note that the type is any,
    because we can't import it here due to circular imports.

    :param adapter: the adapter to register.
    """
    if adapter.can_load():
        LOADER_REGISTRY[adapter.name()].append(adapter)
    if adapter.can_save():
        SAVER_REGISTRY[adapter.name()].append(adapter)


def get_registered_dataframe_types() -> Dict[str, Type]:
    """Returns a dictionary of extension name -> dataframe type.

    :return: the dictionary.
    """
    return {
        extension: type_map[DATAFRAME_TYPE]
        for extension, type_map in DF_TYPE_AND_COLUMN_TYPES.items()
    }


def get_registered_column_types() -> Dict[str, Type]:
    """Returns a dictionary of extension name -> column type.

    :return: the dictionary.
    """
    return {
        extension: type_map[COLUMN_TYPE] for extension, type_map in DF_TYPE_AND_COLUMN_TYPES.items()
    }
