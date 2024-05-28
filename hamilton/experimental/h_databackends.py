# Copyright (c) 2024 databackend contributors (MIT License)
#
# See https://github.com/machow/databackend

import importlib
import sys
from abc import ABCMeta


def _load_class(mod_name: str, cls_name: str):
    mod = importlib.import_module(mod_name)
    return getattr(mod, cls_name)


class _AbstractBackendMeta(ABCMeta):
    def register_backend(cls, mod_name: str, cls_name: str):
        cls._backends.append((mod_name, cls_name))
        cls._abc_caches_clear()


class AbstractBackend(metaclass=_AbstractBackendMeta):
    @classmethod
    def __init_subclass__(cls):
        if not hasattr(cls, "_backends"):
            cls._backends = []

    @classmethod
    def __subclasshook__(cls, subclass):
        for mod_name, cls_name in cls._backends:
            if mod_name not in sys.modules:
                # module isn't loaded, so it can't be the subclass
                # we don't want to import the module to explicitly run the check
                # so skip here.
                continue
            else:
                parent_candidate = _load_class(mod_name, cls_name)
                if issubclass(subclass, parent_candidate):
                    return True

        return NotImplemented


# Below is no longer under the above copyright
import inspect
from typing import Tuple, Type, Union

# TODO add a `_has__dataframe__` attribute for those that implement the
# dataframe interchange protocol


# PyArrow
class AbstractPyArrowDataFrame(AbstractBackend):
    _backends = [("pyarrow", "Table"), ("pyarrow", "RecordBatch")]


class AbstractPyarrowColumn(AbstractBackend):
    _backends = [("pyarrow", "Array"), ("pyarrow", "ChunkedArray")]


# Ibis
class AbstractIbisDataFrame(AbstractBackend):
    _backends = [("ibis.expr.types", "Table")]


class AbstractIbisColumn(AbstractBackend):
    _backends = [("ibis.expr.types", "Column")]


# Pandas
class AbstractPandasDataFrame(AbstractBackend):
    _backends = [("pandas", "DataFrame")]


class AbstractPandasColumn(AbstractBackend):
    _backends = [("pandas", "Series")]


# Polars
class AbstractPolarsDataFrame(AbstractBackend):
    _backends = [("polars", "DataFrame")]


class AbstractPolarsColumn(AbstractBackend):
    _backends = [("polars", "Series")]


# Polars Lazy
class AbstractLazyPolarsDataFrame(AbstractBackend):
    _backends = [("polars", "LazyFrame")]


# Vaex
class AbstractVaexDataFrame(AbstractBackend):
    _backends = [("vaex.dataframe", "DataFrame")]


class AbstractVaexColumn(AbstractBackend):
    _backends = [("vaex.expression", "Expression")]


# Dask
class AbstractDaskDataFrame(AbstractBackend):
    _backends = [("dask.dataframe", "DataFrame")]


class AbstractDaskColumn(AbstractBackend):
    _backends = [("dask.dataframe", "Series")]


# SparkSQL
class AbstractSparkSQLDataFrame(AbstractBackend):
    _backends = [("pyspark.sql", "DataFrame")]


# SparkPandas
class AbstractSparkPandasDataFrame(AbstractBackend):
    _backends = [("pyspark.pandas", "DataFrame")]


class AbstractSparkPandasColumn(AbstractBackend):
    _backends = [("pyspark.pandas", "Series")]


# Geopandas
class AbstractGeoPandasDataFrame(AbstractBackend):
    _backends = [("geopandas", "GeoDataFrame")]


class AbstractGeoPandasColumn(AbstractBackend):
    _backends = [("geopandas", "GeoSeries")]


# cuDF
class AbstractCuDFDataFrame(AbstractBackend):
    _backends = [("cudf", "DataFrame")]


# Modin
class AbstractModinDataFrame(AbstractBackend):
    _backends = [("modin.pandas", "DataFrame")]


def register_backends() -> Tuple[Type, Type]:
    """Register Abstract classes"""
    global DATAFRAME_TYPES
    global COLUMN_TYPES

    abstract_dataframe_types = set()
    abstract_column_types = set()

    h_databackends_module = importlib.import_module(__name__)
    for name, cls in inspect.getmembers(h_databackends_module, inspect.isclass):
        if "DataFrame" in name:
            abstract_dataframe_types.add(cls)
        elif "Column" in name:
            abstract_column_types.add(cls)

    # overwrite sets with Union type objects
    DATAFRAME_TYPES = Union[tuple(abstract_dataframe_types)]
    COLUMN_TYPES = Union[tuple(abstract_column_types)]
    return DATAFRAME_TYPES, COLUMN_TYPES


DATAFRAME_TYPES, COLUMN_TYPES = register_backends()
