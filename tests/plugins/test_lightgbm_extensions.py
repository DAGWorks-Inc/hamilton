import pathlib
from typing import Union

import lightgbm
import numpy as np
import pytest

from hamilton.plugins.lightgbm_extensions import LightGBMFileReader, LightGBMFileWriter


def fitted_lightgbm_model() -> lightgbm.LGBMModel:
    model = lightgbm.LGBMRegressor()
    model.fit([[0], [1]], [0, 1])
    return model


def fitted_lightgbm_booster() -> lightgbm.Booster:
    train = lightgbm.Dataset(np.asarray([[0], [1], [0], [1], [1]]), label=[0, 0, 0, 0, 0])
    booster = lightgbm.train({"objective": "regression"}, train, 1)
    return booster


def fitted_lightgbm_cvbooster() -> lightgbm.CVBooster:
    train = lightgbm.Dataset(np.asarray([[0], [1], [0], [1], [1]]), label=[0, 0, 0, 0, 0])
    results = lightgbm.cv({"objective": "regression"}, train, 1, return_cvbooster=True)
    return results["cvbooster"]


@pytest.mark.parametrize(
    "fitted_lightgbm",
    [fitted_lightgbm_model(), fitted_lightgbm_booster(), fitted_lightgbm_cvbooster()],
)
def test_lightgbm_file_writer(
    fitted_lightgbm: Union[lightgbm.LGBMModel, lightgbm.Booster, lightgbm.CVBooster],
    tmp_path: pathlib.Path,
) -> None:
    model_path = tmp_path / "model.lgbm"
    writer = LightGBMFileWriter(path=model_path)

    metadata = writer.save_data(fitted_lightgbm)

    assert model_path.exists()
    assert metadata["path"] == model_path


@pytest.mark.parametrize(
    "fitted_lightgbm",
    [fitted_lightgbm_model(), fitted_lightgbm_booster(), fitted_lightgbm_cvbooster()],
)
def test_xgboost_model_file_reader(
    fitted_lightgbm: Union[lightgbm.LGBMModel, lightgbm.Booster, lightgbm.CVBooster],
    tmp_path: pathlib.Path,
) -> None:
    model_path = tmp_path / "model.lgbm"
    if isinstance(fitted_lightgbm, lightgbm.LGBMModel):
        fitted_lightgbm.booster_.save_model(filename=model_path)
    else:
        fitted_lightgbm.save_model(filename=model_path)
    reader = LightGBMFileReader(path=model_path)

    model, metadata = reader.load_data(type(fitted_lightgbm))

    assert LightGBMFileReader.applicable_types() == [
        lightgbm.LGBMModel,
        lightgbm.Booster,
        lightgbm.CVBooster,
    ]
    if isinstance(model, lightgbm.Booster):
        assert model.num_trees() > 0
    elif isinstance(model, lightgbm.CVBooster):
        assert len(model.boosters) > 0
    elif isinstance(model, lightgbm.LGBMModel):
        assert model.get_params().get("model_file", False)
    else:
        raise TypeError(f"LightGBMFileReader loaded model of type {type(model)}.")
