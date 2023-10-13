import pathlib

import pytest
import xgboost
from sklearn.utils.validation import check_is_fitted

from hamilton.plugins.xgboost_extensions import (
    XGBoostJsonReader,
    XGBoostJsonWriter,
)


@pytest.fixture
def fitted_xgboost_model() -> xgboost.XGBModel:
    model = xgboost.XGBRegressor()
    model.fit([[0]], [[0]])
    return model


def test_xgboost_model_json_writer(fitted_xgboost_model: xgboost.XGBModel, tmp_path: pathlib.Path) -> None:
    model_path = tmp_path / "model.json"
    writer = XGBoostJsonWriter(path=model_path)

    metadata = writer.save_data(fitted_xgboost_model)

    assert model_path.exists()
    assert metadata["path"] == model_path


def test_xgboost_model_json_reader(
    fitted_xgboost_model: xgboost.XGBModel,
    tmp_path: pathlib.Path
) -> None:
    model_path = tmp_path / "model.json"
    fitted_xgboost_model.save_model(model_path)
    reader = XGBoostJsonReader(model_path)

    model, metadata = reader.load_data(xgboost.XGBRegressor)

    check_is_fitted(model)
    assert XGBoostJsonReader.applicable_types() == [xgboost.XGBModel, xgboost.Booster]


