import pandas as pd
import pytest

from hamilton import driver, node
from hamilton.function_modifiers.base import NodeInjector
from hamilton.plugins.h_pandas import with_columns

from .resources import with_columns_end_to_end


def dummy_fn_with_columns(col_1: pd.Series) -> pd.Series:
    return col_1 + 100


def test_detect_duplicate_nodes():
    node_a = node.Node.from_fn(dummy_fn_with_columns, name="a")
    node_b = node.Node.from_fn(dummy_fn_with_columns, name="a")
    node_c = node.Node.from_fn(dummy_fn_with_columns, name="c")

    if not with_columns._check_for_duplicates([node_a, node_b, node_c]):
        raise (AssertionError)

    if with_columns._check_for_duplicates([node_a, node_c]):
        raise (AssertionError)


def test_select_not_empty():
    error_message = "Please specify at least one column to append or update."

    with pytest.raises(ValueError) as e:
        with_columns(dummy_fn_with_columns)
    assert str(e.value) == error_message


def test_columns_to_pass_and_pass_dataframe_as_raises_error():
    error_message = (
        "You must specify only one of columns_to_pass and "
        "pass_dataframe_as. "
        "This is because specifying pass_dataframe_as injects into "
        "the set of columns, allowing you to perform your own extraction"
        "from the dataframe. We then execute all columns in the sbudag"
        "in order, passing in that initial dataframe. If you want"
        "to reference columns in your code, you'll have to specify "
        "the set of initial columns, and allow the subdag decorator "
        "to inject the dataframe through. The initial columns tell "
        "us which parameters to take from that dataframe, so we can"
        "feed the right data into the right columns."
    )

    with pytest.raises(ValueError) as e:
        with_columns(
            dummy_fn_with_columns, columns_to_pass=["a"], pass_dataframe_as="a", select=["a"]
        )
    assert str(e.value) == error_message


def test_first_parameter_is_dataframe():
    error_message = (
        "First argument has to be a pandas DataFrame. If you wish to use a "
        "different argument, please use `pass_dataframe_as` option."
    )

    def target_fn(upstream_df: int) -> pd.DataFrame:
        return upstream_df

    dummy_node = node.Node.from_fn(target_fn)

    decorator = with_columns(
        dummy_fn_with_columns, columns_to_pass=["col_1"], select=["dummy_fn_with_columns"]
    )
    injectable_params = NodeInjector.find_injectable_params([dummy_node])

    with pytest.raises(ValueError) as e:
        decorator._get_inital_nodes(fn=target_fn, params=injectable_params)

    assert str(e.value) == error_message


def test_create_column_nodes_pass_dataframe():
    def target_fn(some_var: int, upstream_df: pd.DataFrame) -> pd.DataFrame:
        return upstream_df

    dummy_node = node.Node.from_fn(target_fn)

    decorator = with_columns(
        dummy_fn_with_columns, pass_dataframe_as="upstream_df", select=["dummy_fn_with_columns"]
    )
    injectable_params = NodeInjector.find_injectable_params([dummy_node])
    inject_parameter, initial_nodes = decorator._get_inital_nodes(
        fn=target_fn, params=injectable_params
    )

    assert inject_parameter == "upstream_df"
    assert len(initial_nodes) == 0


def test_create_column_nodes_extract_single_columns():
    def dummy_df() -> pd.DataFrame:
        return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14]})

    def target_fn(upstream_df: pd.DataFrame) -> pd.DataFrame:
        return upstream_df

    dummy_node = node.Node.from_fn(target_fn)

    decorator = with_columns(
        dummy_fn_with_columns, columns_to_pass=["col_1"], select=["dummy_fn_with_columns"]
    )
    injectable_params = NodeInjector.find_injectable_params([dummy_node])

    inject_parameter, initial_nodes = decorator._get_inital_nodes(
        fn=target_fn, params=injectable_params
    )

    assert inject_parameter == "upstream_df"
    assert len(initial_nodes) == 1
    assert initial_nodes[0].name == "col_1"
    assert initial_nodes[0].type == pd.Series
    pd.testing.assert_series_equal(
        initial_nodes[0].callable(upstream_df=dummy_df()),
        pd.Series([1, 2, 3, 4]),
        check_names=False,
    )


def test_create_column_nodes_extract_multiple_columns():
    def dummy_df() -> pd.DataFrame:
        return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14]})

    def target_fn(upstream_df: pd.DataFrame) -> pd.DataFrame:
        return upstream_df

    dummy_node = node.Node.from_fn(target_fn)

    decorator = with_columns(
        dummy_fn_with_columns, columns_to_pass=["col_1", "col_2"], select=["dummy_fn_with_columns"]
    )
    injectable_params = NodeInjector.find_injectable_params([dummy_node])

    inject_parameter, initial_nodes = decorator._get_inital_nodes(
        fn=target_fn, params=injectable_params
    )

    assert inject_parameter == "upstream_df"
    assert len(initial_nodes) == 2
    assert initial_nodes[0].name == "col_1"
    assert initial_nodes[1].name == "col_2"
    assert initial_nodes[0].type == pd.Series
    assert initial_nodes[1].type == pd.Series
    pd.testing.assert_series_equal(
        initial_nodes[0].callable(upstream_df=dummy_df()),
        pd.Series([1, 2, 3, 4]),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        initial_nodes[1].callable(upstream_df=dummy_df()),
        pd.Series([11, 12, 13, 14]),
        check_names=False,
    )


def test_no_matching_select_column_error():
    def target_fn(upstream_df: pd.DataFrame) -> pd.DataFrame:
        return upstream_df

    dummy_node = node.Node.from_fn(target_fn)
    select = "wrong_column"

    decorator = with_columns(
        dummy_fn_with_columns, columns_to_pass=["col_1", "col_2"], select=select
    )
    injectable_params = NodeInjector.find_injectable_params([dummy_node])

    error_message = (
        f"No nodes found upstream from select columns: {select} for function: "
        f"{target_fn.__qualname__}"
    )
    with pytest.raises(ValueError) as e:
        decorator.inject_nodes(injectable_params, {}, fn=target_fn)

    assert str(e.value) == error_message


def test_append_into_original_df():
    def dummy_df() -> pd.DataFrame:
        return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14]})

    def target_fn(upstream_df: pd.DataFrame) -> pd.DataFrame:
        return upstream_df

    decorator = with_columns(
        dummy_fn_with_columns, columns_to_pass=["col_1", "col_2"], select=["dummy_fn_with_columns"]
    )
    merge_node = decorator._create_merge_node(upstream_node="upstream_df", node_name="merge_node")

    output_df = merge_node.callable(
        upstream_df=dummy_df(),
        dummy_fn_with_columns=dummy_fn_with_columns(col_1=pd.Series([1, 2, 3, 4])),
    )
    assert merge_node.name == "merge_node"
    assert merge_node.type == pd.DataFrame

    pd.testing.assert_series_equal(output_df["col_1"], pd.Series([1, 2, 3, 4]), check_names=False)
    pd.testing.assert_series_equal(
        output_df["col_2"], pd.Series([11, 12, 13, 14]), check_names=False
    )
    pd.testing.assert_series_equal(
        output_df["dummy_fn_with_columns"], pd.Series([101, 102, 103, 104]), check_names=False
    )


def test_override_original_column_in_df():
    def dummy_df() -> pd.DataFrame:
        return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14]})

    def target_fn(upstream_df: pd.DataFrame) -> pd.DataFrame:
        return upstream_df

    def col_1() -> pd.Series:
        return pd.Series([0, 3, 5, 7])

    decorator = with_columns(col_1, pass_dataframe_as=["upstream_df"], select=["col_1"])
    merge_node = decorator._create_merge_node(upstream_node="upstream_df", node_name="merge_node")

    output_df = merge_node.callable(upstream_df=dummy_df(), col_1=col_1())
    assert merge_node.name == "merge_node"
    assert merge_node.type == pd.DataFrame

    pd.testing.assert_series_equal(output_df["col_1"], pd.Series([0, 3, 5, 7]), check_names=False)
    pd.testing.assert_series_equal(
        output_df["col_2"], pd.Series([11, 12, 13, 14]), check_names=False
    )


def test_assign_custom_namespace_with_columns():
    def target_fn(upstream_df: pd.DataFrame) -> pd.DataFrame:
        return upstream_df

    dummy_node = node.Node.from_fn(target_fn)
    decorator = with_columns(
        dummy_fn_with_columns,
        columns_to_pass=["col_1", "col_2"],
        select=["dummy_fn_with_columns"],
        namespace="dummy_namespace",
    )
    nodes_ = decorator.transform_dag([dummy_node], {}, target_fn)

    assert nodes_[0].name == "target_fn"
    assert nodes_[1].name == "dummy_namespace.col_1"
    assert nodes_[2].name == "dummy_namespace.col_2"
    assert nodes_[3].name == "dummy_namespace.dummy_fn_with_columns"
    assert nodes_[4].name == "dummy_namespace.__append"


def test_end_to_end_with_columns_automatic_extract():
    config_5 = {
        "factor": 5,
    }
    dr = driver.Builder().with_modules(with_columns_end_to_end).with_config(config_5).build()
    result = dr.execute(final_vars=["final_df"], inputs={"user_factor": 1000})["final_df"]

    expected_df = pd.DataFrame(
        {
            "col_1": [1, 2, 3, 4],
            "col_2": [11, 12, 13, 14],
            "col_3": [1, 1, 1, 1],
            "subtract_1_from_2": [10, 10, 10, 10],
            "multiply_3": [5, 5, 5, 5],
            "add_1_by_user_adjustment_factor": [1001, 1002, 1003, 1004],
            "multiply_2_by_upstream_3": [33, 36, 39, 42],
        }
    )
    pd.testing.assert_frame_equal(result, expected_df)

    config_7 = {
        "factor": 7,
    }
    dr = driver.Builder().with_modules(with_columns_end_to_end).with_config(config_7).build()
    result = dr.execute(final_vars=["final_df"], inputs={"user_factor": 1000})["final_df"]

    expected_df = pd.DataFrame(
        {
            "col_1": [1, 2, 3, 4],
            "col_2": [11, 12, 13, 14],
            "col_3": [1, 1, 1, 1],
            "subtract_1_from_2": [10, 10, 10, 10],
            "multiply_3": [7, 7, 7, 7],
            "add_1_by_user_adjustment_factor": [1001, 1002, 1003, 1004],
            "multiply_2_by_upstream_3": [33, 36, 39, 42],
        }
    )
    pd.testing.assert_frame_equal(result, expected_df)


def test_end_to_end_with_columns_pass_dataframe():
    config_5 = {
        "factor": 5,
    }
    dr = driver.Builder().with_modules(with_columns_end_to_end).with_config(config_5).build()

    result = dr.execute(final_vars=["final_df_2"])["final_df_2"]
    expected_df = pd.DataFrame(
        {
            "col_1": [1, 2, 3, 4],
            "col_2": [11, 12, 13, 14],
            "col_3": [0, 2, 4, 6],
            "multiply_3": [0, 10, 20, 30],
        }
    )
    pd.testing.assert_frame_equal(result, expected_df)
