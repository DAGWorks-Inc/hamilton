import dataclasses
from typing import Any, Collection, Dict, Type

from hamilton import registry
from hamilton.io.data_adapters import DataSaver

try:
    import mlflow
except ImportError:
    raise ImportError("Please install mlflow to use this plugin")

"""
While it can be valid to wrap the entire code within the start_run block,
this is not recommended. If there as in issue with the training of the
model or any other portion of code that is unrelated to MLflow-related
actions, an empty or partially-logged run will be created, which will
necessitate manual cleanup of the invalid run. It is best to keep the
training execution outside of the run context block to ensure that the
loggable content (parameters, metrics, artifacts, and the model) are
fully materialized prior to logging.
"""


@dataclasses.dataclass
class MLFLowSaver(DataSaver):
    """Our MLFlow Materializer"""

    experiment_name: str
    model_type: str  # e.g. "pyfunc", "sklearn", "spark", "onnx", "pytorch", "tensorflow", "xgboost"
    run_name: str = None
    artifact_path: str = "models"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [object]

    @classmethod
    def name(cls) -> str:
        return "mlflow"

    def save_data(self, model: object) -> Dict[str, Any]:
        mlflow.set_experiment(self.experiment_name)
        # Initiate the MLflow run context
        with mlflow.start_run(run_name=self.run_name) as run:
            # Log the parameters used for the model fit
            # mlflow.log_params(data["params"])
            # Log the error metrics that were calculated
            # mlflow.log_metrics(data["metrics"])

            # Log an instance of the trained model for later use
            ml_logger = getattr(mlflow, self.model_type)
            model_info = ml_logger.log_model(
                model,
                # data["trained_model"],
                self.artifact_path,
                # input_example=data["input_example"],
            )
        return {
            "model_info": model_info.__dict__,  # return some metadata
            "run_info": run.to_dictionary(),
            "mlflow_uri": f"{mlflow.get_tracking_uri()}/#/experiments/{run.info.experiment_id}/runs/{run.info.run_id}",
        }


adapters = [MLFLowSaver]

for adapter in adapters:
    registry.register_adapter(adapter)


COLUMN_FRIENDLY_DF_TYPE = False
