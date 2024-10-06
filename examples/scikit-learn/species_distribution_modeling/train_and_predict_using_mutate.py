import numpy as np
import numpy.typing as npt
from sklearn import svm
from sklearn.utils._bunch import Bunch

from hamilton.function_modifiers import apply_to, mutate, source, value


def prediction_train(train_cover_std: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return train_cover_std


def prediction_test(train_cover_std: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    return train_cover_std


@mutate(prediction_train, prediction_test, nu=value(0.1), kernel=value("rbf"), gamma=value(0.5))
def _OneClassSVM_model(
    training_set: npt.NDArray[np.float64], nu: float, kernel: str, gamma: float
) -> svm.OneClassSVM:
    clf = svm.OneClassSVM(nu=nu, kernel=kernel, gamma=gamma)
    clf.fit(training_set)
    return clf


@mutate(
    apply_to(
        prediction_train,
        underlying_data=source("coverages_land"),
        mean=source("mean"),
        std=source("std"),
    ),
    apply_to(
        prediction_test,
        underlying_data=source("test_cover_std"),
        mean=source("mean"),
        std=source("std"),
    ),
)
def _decision_function(
    model: svm.OneClassSVM,
    underlying_data: npt.NDArray[np.float64],
    mean: npt.NDArray[np.float64],
    std: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    return model.decision_function((underlying_data - mean) / std)


@mutate(prediction_train, idx=source("idx"), data=source("data"))
def _prediction_step(
    decision: npt.NDArray[np.float64], idx: npt.NDArray[np.float64], data: Bunch
) -> npt.NDArray[np.float64]:
    Z = decision.min() * np.ones((data.Ny, data.Nx), dtype=np.float64)
    Z[idx[0], idx[1]] = decision
    return Z
