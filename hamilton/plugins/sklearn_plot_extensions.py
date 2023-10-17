import dataclasses
from os import PathLike
from typing import Any, Collection, Dict, Tuple, Type, Union


try:
    import sklearn.metrics #import ConfusionMatrixDisplay, plot_confusion_matrix
except ImportError:
    raise NotImplementedError("scikit-learn is not installed.")

try:
    from sklearn.exceptions import NotFittedError
except ImportError:
    raise NotImplementedError("scikit-learn is not installed.")

try:
    from matplotlib import pyplot as plt
except ImportError:
    raise NotImplementedError("matplotlib is not installed")

from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataSaver


SKLEARN_PLOT_TYPES = [sklearn.metrics.ConfusionMatrixDisplay, sklearn.metrics.DetCurveDisplay, sklearn.metrics.PrecisionRecallDisplay, sklearn.metrics.PredictionErrorDisplay, sklearn.metrics.RocCurveDisplay]
SKLEARN_PLOT_TYPES_ANNOTATION = Union[sklearn.metrics.ConfusionMatrixDisplay, sklearn.metrics.DetCurveDisplay, sklearn.metrics.PrecisionRecallDisplay, sklearn.metrics.PredictionErrorDisplay, sklearn.metrics.RocCurveDisplay]

@dataclasses.dataclass
class ConfusionMatrixPlotSaver(DataSaver):

    path: Union[str, PathLike]

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return SKLEARN_PLOT_TYPES

    def save_plot(self, data: SKLEARN_PLOT_TYPES_ANNOTATION):
        try:
            plt.savefig(self.path, dpi=200)
        except NotFittedError as e:
            return utils.get_exception_details(e)
        return utils.get_file_metadata(self.path)
    
    @classmethod
    def name(cls) -> str:
        return ""  # TODO: Decide on name
    
def register_data_loaders():
    """Function to register the data loaders for this extension."""
    registry.register_adapter(ConfusionMatrixPlotSaver)

register_data_loaders()