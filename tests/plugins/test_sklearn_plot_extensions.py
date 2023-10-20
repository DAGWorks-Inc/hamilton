import pathlib
import sys

import numpy as np
import pytest
import sklearn.calibration as calib
import sklearn.metrics as metrics
from sklearn.datasets import load_diabetes, make_classification
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC

from hamilton.plugins.sklearn_plot_extensions import SklearnPlotSaver


@pytest.fixture
def confusion_matrix_display() -> metrics.ConfusionMatrixDisplay:
    X, y = make_classification(random_state=0)
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
    clf = SVC(random_state=0)
    clf.fit(X_train, y_train)
    predictions = clf.predict(X_test)
    cm = metrics.confusion_matrix(y_test, predictions, labels=clf.classes_)
    cm_display = metrics.ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=clf.classes_)
    return cm_display


@pytest.fixture
def det_curve_display() -> metrics.DetCurveDisplay:
    X, y = make_classification(n_samples=1000, random_state=0)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=0)
    clf = SVC(random_state=0).fit(X_train, y_train)
    y_pred = clf.decision_function(X_test)
    fpr, fnr, _ = metrics.det_curve(y_test, y_pred)
    detcurve = metrics.DetCurveDisplay(fpr=fpr, fnr=fnr, estimator_name="SVC")
    return detcurve


@pytest.fixture
def precision_recall_display() -> metrics.PrecisionRecallDisplay:
    X, y = make_classification(n_samples=1000, random_state=0)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=0)
    clf = SVC(random_state=0).fit(X_train, y_train)
    clf.fit(X_train, y_train)
    predictions = clf.predict(X_test)
    precision, recall, _ = metrics.precision_recall_curve(y_test, predictions)
    precision_recall = metrics.PrecisionRecallDisplay(precision=precision, recall=recall)
    return precision_recall


if sys.version_info >= (3, 8):

    @pytest.fixture
    def prediction_error_display() -> metrics.PredictionErrorDisplay:
        X, y = load_diabetes(return_X_y=True)
        ridge = Ridge().fit(X, y)
        y_pred = ridge.predict(X)
        pred_error = metrics.PredictionErrorDisplay.from_predictions(y_true=y, y_pred=y_pred)
        return pred_error

else:
    pass


@pytest.fixture
def roc_curve_display() -> metrics.RocCurveDisplay:
    y = np.array([0, 0, 1, 1])
    pred = np.array([0.1, 0.4, 0.35, 0.8])
    fpr, tpr, threshold = metrics.roc_curve(y, pred)
    roc_auc = metrics.auc(fpr, tpr)
    roccurve = metrics.RocCurveDisplay(
        fpr=fpr, tpr=tpr, roc_auc=roc_auc, estimator_name="example estimator"
    )
    return roccurve


@pytest.fixture
def calibration_display() -> calib.CalibrationDisplay:
    X, y = make_classification(random_state=0)
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
    clf = LogisticRegression(random_state=0)
    clf.fit(X_train, y_train)
    LogisticRegression(random_state=0)
    y_prob = clf.predict_proba(X_test)[:, 1]
    prob_true, prob_pred = calib.calibration_curve(y_test, y_prob, n_bins=10)
    disp = calib.CalibrationDisplay(prob_true, prob_pred, y_prob)
    return disp


def test_cm_plot_saver(
    confusion_matrix_display: metrics.ConfusionMatrixDisplay, tmp_path: pathlib.Path
) -> None:
    plot_path = tmp_path / "cm_plot.png"
    writer = SklearnPlotSaver(path=plot_path)

    metadata = writer.save_data(confusion_matrix_display)

    assert plot_path.exists()
    assert metadata["path"] == plot_path


def test_det_curve_display(
    det_curve_display: metrics.DetCurveDisplay, tmp_path: pathlib.Path
) -> None:
    plot_path = tmp_path / "det_curve_plot.png"
    writer = SklearnPlotSaver(path=plot_path)

    metadata = writer.save_data(det_curve_display)

    assert plot_path.exists()
    assert metadata["path"] == plot_path


def test_precision_recall_display(
    precision_recall_display: metrics.PrecisionRecallDisplay, tmp_path: pathlib.Path
) -> None:
    plot_path = tmp_path / "precision_recall.png"
    writer = SklearnPlotSaver(path=plot_path)

    metadata = writer.save_data(precision_recall_display)

    assert plot_path.exists()
    assert metadata["path"] == plot_path


def test_prediction_error_display(
    prediction_error_display: metrics.PredictionErrorDisplay, tmp_path: pathlib.Path
) -> None:
    plot_path = tmp_path / "prediction_error_plot.png"
    writer = SklearnPlotSaver(path=plot_path)

    metadata = writer.save_data(prediction_error_display)

    assert plot_path.exists()
    assert metadata["path"] == plot_path


def test_roc_curve_display(
    roc_curve_display: metrics.RocCurveDisplay, tmp_path: pathlib.Path
) -> None:
    plot_path = tmp_path / "roc_curve.png"
    writer = SklearnPlotSaver(path=plot_path)

    metadata = writer.save_data(roc_curve_display)

    assert plot_path.exists()
    assert metadata["path"] == plot_path


def test_calibration_display(
    calibration_display: calib.CalibrationDisplay, tmp_path: pathlib.Path
) -> None:
    plot_path = tmp_path / "calibration_curve.png"
    writer = SklearnPlotSaver(path=plot_path)

    metadata = writer.save_data(calibration_display)

    assert plot_path.exists()
    assert metadata["path"] == plot_path
