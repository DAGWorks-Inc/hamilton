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
    mean_: npt.NDArray[np.float64],
    std_: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    return model.decision_function((underlying_data - mean_) / std_)


def _prediction_step(
    decision: npt.NDArray[np.float64], idx_: npt.NDArray[np.float64], data_: Bunch
) -> npt.NDArray[np.float64]:
    _Z = decision.min() * np.ones((data_.Ny, data_.Nx), dtype=np.float64)
    _Z[idx_[0], idx_[1]] = decision
    return _Z


@pipe_output(
    step(_OneClassSVM_model, nu=value(0.1), kernel=value("rbf"), gamma=value(0.5)),
    step(
        _decision_function,
        underlying_data=source("coverages_land"),
        mean_=source("mean"),
        std_=source("std"),
    ),
    step(_prediction_step, idx_=source("idx"), data_=source("data")),
)
def prediction_train(train_cover_std: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return train_cover_std


@pipe_output(
    step(_OneClassSVM_model, nu=value(0.1), kernel=value("rbf"), gamma=value(0.5)),
    step(
        _decision_function,
        underlying_data=source("test_cover_std"),
        mean_=source("mean"),
        std_=source("std"),
    ),
)
def prediction_test(train_cover_std: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return train_cover_std
