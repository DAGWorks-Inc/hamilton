import logging

import pandas as pd
import pytest

from hamilton import base
from hamilton.driver import Driver
from hamilton.experimental.h_cache import (
    CachingAdapter,
    read_feather,
    read_json,
    read_parquet,
    write_feather,
    write_json,
    write_parquet,
)
from tests import nodes


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )


def _read_write_df_test(df, filepath, read_fn, write_fn):
    write_fn(df, filepath)
    pd.testing.assert_frame_equal(read_fn(filepath), df)


def test_feather(df, tmp_path):
    _read_write_df_test(df, tmp_path / "file.feather", read_feather, write_feather)


def test_parquet(df, tmp_path):
    _read_write_df_test(df, tmp_path / "file.parquet", read_parquet, write_parquet)


def test_json(tmp_path):
    data = {
        "a": 1,
        "b": 2,
    }
    filepath = tmp_path / "file.json"
    write_json(data, filepath)
    assert read_json(filepath) == data


def test_unknown_format(tmp_path):
    cache_path = str(tmp_path)
    adapter = CachingAdapter(cache_path, base.DictResult())
    data = {"a": 1, "b": 2}
    filepath = tmp_path / "file.xyz"
    with pytest.raises(ValueError) as err:
        adapter._write_cache(data, "xyz", filepath)
    assert str(err.value) == "invalid cache format: xyz"
    with pytest.raises(ValueError) as err:
        adapter._read_cache("xyz", filepath)
    assert str(err.value) == "invalid cache format: xyz"


def test_init_default_readers_writers(tmp_path):
    cache_path = str(tmp_path / "data")
    adapter = CachingAdapter(cache_path, base.DictResult())
    assert adapter.writers == {
        "json": write_json,
        "feather": write_feather,
        "parquet": write_parquet,
    }
    assert adapter.readers == {
        "json": read_json,
        "feather": read_feather,
        "parquet": read_parquet,
    }


def read_str(filepath: str) -> str:
    with open(filepath, "r", encoding="utf8") as file:
        return file.read()


def write_str(data: str, filepath: str) -> None:
    with open(filepath, "w", encoding="utf8") as file:
        file.write(data)


def test_caching(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    cache_path = str(tmp_path)
    adapter = CachingAdapter(
        cache_path,
        base.DictResult(),
        readers={"str": read_str},
        writers={"str": write_str},
    )
    dr = Driver(
        {"initial": "Hello, World!"},
        nodes,
        adapter=adapter,
    )
    assert dr.execute(["both"])["both"] == {
        "lower": "hello, world!",
        "upper": "HELLO, WORLD!",
    }
    assert {"lowercased", "uppercased", "both"} == {
        rec.message for rec in caplog.records
    }

    assert read_str(str(tmp_path / "lowercased.str")) == "hello, world!"
    assert read_str(str(tmp_path / "uppercased.str")) == "HELLO, WORLD!"
    assert (
        read_str(str(tmp_path / "both.json"))
        == '{"lower": "hello, world!", "upper": "HELLO, WORLD!"}'
    )

    caplog.clear()

    assert dr.execute(["both"])["both"] == {
        "lower": "hello, world!",
        "upper": "HELLO, WORLD!",
    }
    assert not {rec.message for rec in caplog.records}

    caplog.clear()

    # now we force-compute one of the dependencies
    adapter = CachingAdapter(
        cache_path,
        base.DictResult(),
        readers={"str": read_str},
        writers={"str": write_str},
        force_compute={"lowercased"},
    )
    dr = Driver(
        {"initial": "Hello, World!"},
        nodes,
        adapter=adapter,
    )
    assert dr.execute(["both"])["both"] == {
        "lower": "hello, world!",
        "upper": "HELLO, WORLD!",
    }
    # both the forced node and the node dependent on it is computed
    assert {"lowercased", "both"} == {rec.message for rec in caplog.records}
