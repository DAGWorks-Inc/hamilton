import numpy as np
import numpy.typing as npt
from sklearn import svm
from sklearn.utils._bunch import Bunch

from hamilton.function_modifiers import pipe_output, source, step, value


# TODO: add another model or two and use `.when` to showcase that this can be customizable @execution
def _OneClassSVM_model(
    training_set: npt.NDArray[np.float64], nu: float, kernel: str, gamma: float
) -> svm.OneClassSVM:
    clf = svm.OneClassSVM(nu=nu, kernel=kernel, gamma=gamma)
    clf.fit(training_set)
    return clf


def _decision_function(
    model: svm.OneClassSVM,
    underlying_data: npt.NDArray[np.float64],
    _mean: npt.NDArray[np.float64],
    _std: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    return model.decision_function((underlying_data - _mean) / _std)


def _prediction_step(
    decision: npt.NDArray[np.float64], _idx: npt.NDArray[np.float64], _data: Bunch
) -> npt.NDArray[np.float64]:
    Z = decision.min() * np.ones((_data.Ny, _data.Nx), dtype=np.float64)
    Z[_idx[0], _idx[1]] = decision
    return Z


@pipe_output(
    step(_OneClassSVM_model, nu=value(0.1), kernel=value("rbf"), gamma=value(0.5)),
    step(
        _decision_function,
        underlying_data=source("coverages_land"),
        _mean=source("mean"),
        _std=source("std"),
    ),
    step(_prediction_step, _idx=source("idx"), _data=source("data")),
)
def prediction_train(train_cover_std: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return train_cover_std


@pipe_output(
    step(_OneClassSVM_model, nu=value(0.1), kernel=value("rbf"), gamma=value(0.5)),
    step(
        _decision_function,
        underlying_data=source("test_cover_std"),
        _mean=source("mean"),
        _std=source("std"),
    ),
)
def prediction_test(train_cover_std: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return train_cover_std
