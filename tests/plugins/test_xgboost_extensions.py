import pathlib

import pytest
import xgboost
from sklearn.utils.validation import check_is_fitted

from hamilton.io.utils import FILE_METADATA
from hamilton.plugins.xgboost_extensions import XGBoostJsonReader, XGBoostJsonWriter


@pytest.fixture
def fitted_xgboost_model() -> xgboost.XGBModel:
    model = xgboost.XGBRegressor()
    model.fit([[0]], [[0]])
    return model


@pytest.fixture
def fitted_xgboost_booster() -> xgboost.Booster:
    dtrain = xgboost.DMatrix([[0]], label=[[0]])
    booster = xgboost.train({"objective": "binary:logistic"}, dtrain, 1)
    return booster


def test_xgboost_model_json_writer(
    fitted_xgboost_model: xgboost.XGBModel, tmp_path: pathlib.Path
) -> None:
    model_path = tmp_path / "model.json"
    writer = XGBoostJsonWriter(path=model_path)

    metadata = writer.save_data(fitted_xgboost_model)

    assert model_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(model_path)


def test_xgboost_model_json_reader(
    fitted_xgboost_model: xgboost.XGBModel, tmp_path: pathlib.Path
) -> None:
    model_path = tmp_path / "model.json"
    fitted_xgboost_model.save_model(model_path)
    reader = XGBoostJsonReader(model_path)

    model, metadata = reader.load_data(xgboost.XGBRegressor)

    check_is_fitted(model)
    assert XGBoostJsonReader.applicable_types() == [xgboost.XGBModel, xgboost.Booster]


def test_xgboost_booster_json_writer(
    fitted_xgboost_booster: xgboost.Booster, tmp_path: pathlib.Path
) -> None:
    booster_path = tmp_path / "booster.json"
    writer = XGBoostJsonWriter(path=booster_path)

    metadata = writer.save_data(fitted_xgboost_booster)

    assert booster_path.exists()
    assert metadata[FILE_METADATA]["path"] == str(booster_path)


def test_xgboost_booster_json_reader(
    fitted_xgboost_booster: xgboost.Booster, tmp_path: pathlib.Path
) -> None:
    booster_path = tmp_path / "booster.json"
    fitted_xgboost_booster.save_model(booster_path)
    reader = XGBoostJsonReader(booster_path)

    booster, metadata = reader.load_data(xgboost.Booster)

    assert len(booster.get_dump()) > 0
    assert XGBoostJsonReader.applicable_types() == [xgboost.XGBModel, xgboost.Booster]
