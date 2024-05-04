import pandas as pd
import pytest
from hamilton_code import data_science

from hamilton.driver import Builder


@pytest.fixture
def dummy_data():
    return pd.DataFrame(
        {
            "engines": [1, 2, 3],
            "crew": [4, 5, 6],
            "passenger_capacity": [5, 6, 7],
            "price": [120, 290, 30],
        }
    )


@pytest.fixture
def dummy_inputs():
    return {
        "test_size": 0.2,
        "random_state": 3,
        "features": ["engines", "passenger_capacity", "crew"],
    }


def test_split_data(dummy_data, dummy_inputs):
    results = data_science.split_data(create_model_input_table=dummy_data, **dummy_inputs)
    assert len(results["X_train"]) == 2
    assert len(results["y_train"]) == 2
    assert len(results["X_test"]) == 1
    assert len(results["y_test"]) == 1


def test_split_data_missing_price(dummy_data, dummy_inputs):
    dummy_data_missing_price = dummy_data.drop(columns="price")
    with pytest.raises(KeyError) as e_info:
        X_train, X_test, y_train, y_test = data_science.split_data(
            create_model_input_table=dummy_data_missing_price, **dummy_inputs
        )

    assert "price" in str(e_info.value)


def test_data_science_pipeline(dummy_data, dummy_inputs):
    driver = Builder().with_modules(data_science).build()
    results = driver.execute(
        ["evaluate_model"],
        inputs=dict(
            create_model_input_table=dummy_data,
            **dummy_inputs,
        ),
    )

    assert results["evaluate_model"]
