import json

from hamilton import base, driver
from hamilton.plugins import h_narwhals, h_polars

from .resources import narwhals_example


def test_pandas():
    # pandas
    dr = (
        driver.Builder()
        .with_config({"load": "pandas"})
        .with_modules(narwhals_example)
        .with_adapters(
            h_narwhals.NarwhalsAdapter(),
            h_narwhals.NarwhalsDataFrameResultBuilder(base.PandasDataFrameResult()),
        )
        .build()
    )
    r = dr.execute(
        [narwhals_example.group_by_mean, narwhals_example.example1], inputs={"col_name": "a"}
    )
    assert r.to_dict() == {
        "example1": {0: 3, 1: 3, 2: 3},
        "group_by_mean.a": {0: 1, 1: 2, 2: 3},
        "group_by_mean.b": {0: 4.5, 1: 6.5, 2: 8.0},
    }


def test_polars():
    # polars
    dr = (
        driver.Builder()
        .with_config({"load": "polars"})
        .with_modules(narwhals_example)
        .with_adapters(
            h_narwhals.NarwhalsAdapter(),
            h_narwhals.NarwhalsDataFrameResultBuilder(h_polars.PolarsDataFrameResult()),
        )
        .build()
    )
    r = dr.execute(
        [narwhals_example.group_by_mean, narwhals_example.example1], inputs={"col_name": "a"}
    )
    assert json.loads(r.write_json()) == {
        "columns": [
            {
                "datatype": {
                    "Struct": [{"dtype": "Int64", "name": "a"}, {"dtype": "Float64", "name": "b"}]
                },
                "name": "group_by_mean",
                "values": [
                    {
                        "bit_settings": "SORTED_ASC",
                        "datatype": "Int64",
                        "name": "a",
                        "values": [1, 2, 3],
                    },
                    {
                        "bit_settings": "",
                        "datatype": "Float64",
                        "name": "b",
                        "values": [4.5, 6.5, 8.0],
                    },
                ],
            },
            {
                "bit_settings": "SORTED_ASC",
                "datatype": "Int32",
                "name": "example1",
                "values": [3, 3, 3],
            },
        ]
    }
