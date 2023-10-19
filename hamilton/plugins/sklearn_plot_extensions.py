import dataclasses
from os import PathLike
from typing import Any, Collection, Dict, Type, Union

try:
    import sklearn.metrics
except ImportError:
    raise NotImplementedError("scikit-learn is not installed.")


from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataSaver

SKLEARN_PLOT_TYPES = [
    sklearn.metrics.ConfusionMatrixDisplay,
    sklearn.metrics.DetCurveDisplay,
    sklearn.metrics.PrecisionRecallDisplay,
    sklearn.metrics.PredictionErrorDisplay,
    sklearn.metrics.RocCurveDisplay,
]
SKLEARN_PLOT_TYPES_ANNOTATION = Union[
    sklearn.metrics.ConfusionMatrixDisplay,
    sklearn.metrics.DetCurveDisplay,
    sklearn.metrics.PrecisionRecallDisplay,
    sklearn.metrics.PredictionErrorDisplay,
    sklearn.metrics.RocCurveDisplay,
]


@dataclasses.dataclass
class SklearnPlotSaver(DataSaver):

    path: Union[str, PathLike]

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return SKLEARN_PLOT_TYPES

    def save_data(self, data: SKLEARN_PLOT_TYPES_ANNOTATION) -> Dict[str, Any]:
        data.plot()
        data.figure_.savefig(self.path, dpi=200)
        return utils.get_file_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "png"


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    registry.register_adapter(SklearnPlotSaver)


register_data_loaders()
