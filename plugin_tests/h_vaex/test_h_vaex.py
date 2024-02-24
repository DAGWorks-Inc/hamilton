import numpy as np
import vaex

from hamilton import base, driver
from hamilton.plugins import h_vaex, vaex_extensions  # noqa F401

from .resources import functions


def test_vaex_column_from_expression():
    adapter = base.SimplePythonGraphAdapter(result_builder=h_vaex.VaexDataFrameResult())
    dr = driver.Driver({}, functions, adapter=adapter)
    result_df = dr.execute(["a", "b", "a_plus_b_expression"])
    assert isinstance(result_df, vaex.dataframe.DataFrame)
    np.testing.assert_allclose(result_df["a_plus_b_expression"].to_numpy(), [3, 5, 7])


def test_vaex_column_from_nparray():
    adapter = base.SimplePythonGraphAdapter(result_builder=h_vaex.VaexDataFrameResult())
    dr = driver.Driver({}, functions, adapter=adapter)
    result_df = dr.execute(["a", "b", "a_plus_b_nparray"])
    assert isinstance(result_df, vaex.dataframe.DataFrame)
    np.testing.assert_allclose(result_df["a_plus_b_nparray"].to_numpy(), [3, 5, 7])


def test_vaex_scalar_among_columns():
    adapter = base.SimplePythonGraphAdapter(result_builder=h_vaex.VaexDataFrameResult())
    dr = driver.Driver({}, functions, adapter=adapter)
    result_df = dr.execute(["a", "b", "a_mean"])
    assert isinstance(result_df, vaex.dataframe.DataFrame)
    np.testing.assert_allclose(result_df["a_mean"].to_numpy(), [2, 2, 2])


def test_vaex_only_scalars():
    adapter = base.SimplePythonGraphAdapter(result_builder=h_vaex.VaexDataFrameResult())
    dr = driver.Driver({}, functions, adapter=adapter)
    result_df = dr.execute(["a_mean", "b_mean"])
    assert isinstance(result_df, vaex.dataframe.DataFrame)
    np.testing.assert_allclose(result_df["a_mean"].to_numpy(), [2])
    np.testing.assert_allclose(result_df["b_mean"].to_numpy(), [3])


def test_vaex_df_among_columns():
    adapter = base.SimplePythonGraphAdapter(result_builder=h_vaex.VaexDataFrameResult())
    dr = driver.Driver({}, functions, adapter=adapter)
    result_df = dr.execute(["a", "b", "ab_as_df"])
    assert isinstance(result_df, vaex.dataframe.DataFrame)
    np.testing.assert_allclose(result_df["a_in_df"].to_numpy(), result_df["a"].to_numpy())
    np.testing.assert_allclose(result_df["b_in_df"].to_numpy(), result_df["b"].to_numpy())
