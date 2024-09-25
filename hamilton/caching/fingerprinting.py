import base64
import functools
import hashlib
import logging
from collections.abc import Mapping, Sequence, Set
from types import NoneType
from typing import Dict

from hamilton.experimental import h_databackends

logger = logging.getLogger(__name__)


MAX_DEPTH = 4
UNHASHABLE = "<unhashable>"
NONE_HASH = "<none>"


def set_max_depth(depth: int) -> None:
    """Set the maximum recursion depth for fingerprinting non-supported types.

    :param depth: The maximum depth for fingerprinting.
    """
    global MAX_DEPTH
    MAX_DEPTH = depth


def _compact_hash(digest: bytes) -> str:
    """Compact the hash to a string that's safe to pass around.

    NOTE this is particularly relevant for the Hamilton UI and
    passing hashes/fingerprints through web services.
    """
    return base64.urlsafe_b64encode(digest).decode()


@functools.singledispatch
def hash_value(obj, *args, depth=0, **kwargs) -> str:
    """Fingerprinting strategy that computes a hash of the
    full Python object.

    The default case hashes the `__dict__` attribute of the
    object (recursive).
    """
    if hasattr(obj, "__dict__") and depth < MAX_DEPTH:
        depth += 1
        return hash_value(obj.__dict__, depth=depth)

    if depth >= MAX_DEPTH:
        logger.warning(
            f"Currently versioning object of type `{type(obj)}` and hiting recursion depth {depth}. "
            f"To avoid data version collisions, register a data versioning function for type `{type(obj)}` "
            "or increase the module constant `hamilton.io.fingeprinting.MAX_DEPTH`. "
            "See the Hamilton documentation Concepts page about caching for details."
        )

    return UNHASHABLE


@hash_value.register(NoneType)
def hash_none(obj, *args, **kwargs) -> str:
    """Hash for None is <none>"""
    return NONE_HASH


@hash_value.register(str)
@hash_value.register(int)
@hash_value.register(float)
@hash_value.register(bool)
def hash_primitive(obj, *args, **kwargs) -> str:
    """Convert the primitive to a string and hash it"""
    hash_object = hashlib.md5(str(obj).encode())
    return _compact_hash(hash_object.digest())


@hash_value.register(bytes)
def hash_bytes(obj, *args, **kwargs) -> str:
    """Convert the primitive to a string and hash it"""
    hash_object = hashlib.md5(obj)
    return _compact_hash(hash_object.digest())


@hash_value.register(Sequence)
def hash_sequence(obj, *args, **kwargs) -> str:
    """Hash each object of the sequence.

    Orders matters for the hash since orders matters in a sequence.
    """
    hash_object = hashlib.sha224()
    for elem in obj:
        hash_object.update(hash_value(elem).encode())

    return _compact_hash(hash_object.digest())


def hash_unordered_mapping(obj, *args, **kwargs) -> str:
    """

    When hashing an unordered mapping, the two following dict have the same hash.

    .. code-block:: python

        foo = {"key": 3, "key2": 13}
        bar = {"key2": 13, "key": 3}

        hash_mapping(foo) == hash_mapping(bar)
    """

    hashed_mapping: Dict[str, str] = {
        hash_value(key): hash_value(value) for key, value in obj.items()
    }

    hash_object = hashlib.sha224()
    for key, value in sorted(hashed_mapping.items()):
        hash_object.update(key.encode())
        hash_object.update(value.encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(Mapping)
def hash_mapping(obj, *, ignore_order: bool = True, **kwargs) -> str:
    """Hash each key then its value.

    The mapping is always sorted first because order shouldn't matter
    in a mapping.

    NOTE Since Python 3.7, dictionary store insertion order. However, this
    function assumes that they key order doesn't matter to uniquely identify
    the dictionary.

    .. code-block:: python

        foo = {"key": 3, "key2": 13}
        bar = {"key2": 13, "key": 3}

        hash_mapping(foo) == hash_mapping(bar)

    """
    if ignore_order:
        return hash_unordered_mapping(obj)

    hash_object = hashlib.sha224()
    for key, value in obj.items():
        hash_object.update(hash_value(key).encode())
        hash_object.update(hash_value(value).encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(Set)
def hash_set(obj, *args, **kwargs) -> str:
    """Hash each element of the set, then sort hashes, and
    create a hash of hashes.

    For the same objects in the set, the hashes will be the
    same.
    """
    hashes = [hash_value(elem) for elem in obj]
    sorted_hashes = sorted(hashes)

    hash_object = hashlib.sha224()
    for hash in sorted_hashes:
        hash_object.update(hash.encode())

    return _compact_hash(hash_object.digest())


@hash_value.register(h_databackends.AbstractPandasColumn)
@hash_value.register(h_databackends.AbstractPandasDataFrame)
def hash_pandas_obj(obj, *args, **kwargs) -> str:
    """Convert a pandas dataframe, series, or index to
    a dictionary of {index: row_hash} then hash it.

    Given the hashing for mappings, the physical ordering or rows doesn't matter.
    For example, if the index is a date, the hash will represent the {date: row_hash},
    and won't preserve how dates were ordered in the DataFrame.
    """
    from pandas.util import hash_pandas_object

    hash_per_row = hash_pandas_object(obj)
    return hash_mapping(hash_per_row.to_dict(), ignore_order=False)


@hash_value.register(h_databackends.AbstractPolarsDataFrame)
def hash_polars_dataframe(obj, *args, **kwargs) -> str:
    """Convert a polars dataframe, series, or index to
    a list of hashes then hash it.
    """
    hash_per_row = obj.hash_rows()
    return hash_sequence(hash_per_row.to_list())


@hash_value.register(h_databackends.AbstractPolarsColumn)
def hash_polars_column(obj, *args, **kwargs) -> str:
    """Promote the single Series to a dataframe and hash it"""
    return hash_polars_dataframe(obj.to_frame())
