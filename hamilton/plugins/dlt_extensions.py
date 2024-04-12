import dataclasses
from typing import Any, Collection, Dict, Iterable, Literal, Optional, Type, Union

import dlt
import pandas as pd
from dlt import TSecretValue

# importing TDestinationReferenceArg fails if Destination isn't imported
from dlt.common.destination import Destination, TDestinationReferenceArg  # noqa: F401

from hamilton import registry
from hamilton.io.data_adapters import DataSaver

try:
    import pyarrow as pa
except ModuleNotFoundError:
    SAVER_TYPES = [Iterable, pd.DataFrame]
else:
    SAVER_TYPES = [Iterable, pd.DataFrame, pa.Table, pa.RecordBatch]


@dataclasses.dataclass
class DltSaver(DataSaver):
    """Materialize results using a dlt pipeline with the specified destination.

    In reference to an Extract, Transform, Load (ETL) pipeline, here, the Hamilton
    dataflow is responsible for Transform, and `DltDestination` for Load.
    """

    #
    table_name: Union[str, dict]
    destination: Optional[TDestinationReferenceArg] = None
    # kwargs for dlt.pipeline()
    pipeline_name: Optional[str] = None
    pipelines_dir: Optional[str] = None
    pipeline_salt: Optional[TSecretValue] = None
    staging: Optional[TDestinationReferenceArg] = None
    dataset_name: Optional[str] = None
    import_schema_path: Optional[str] = None
    export_schema_path: Optional[str] = None
    full_refresh: bool = False
    credentials: Any = None
    # pass a pipeline directly instead of creating it from kwargs
    pipeline: Optional[dlt.Pipeline] = None
    # kwargs for pipeline.run()
    write_disposition: Optional[Literal["skip", "append", "replace", "merge"]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return SAVER_TYPES

    def _get_kwargs(self):
        """Utility to get kwargs from the class"""
        kwargs = {}
        for field in dataclasses.fields(self):
            if field.name in ["write_disposition", "pipeline", "table_name"]:
                continue

            field_value = getattr(self, field.name)
            if field_value != field.default:
                kwargs[field.name] = field_value

        return kwargs

    def save_data(self, data) -> Dict[str, Any]:
        """
        ref:
            tables: https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas
            standalone resources: https://dlthub.com/docs/general-usage/resource#load-resources
            dynamic resources: https://dlthub.com/docs/general-usage/source#create-resources-dynamically
        """

        # check if a pipeline was passed directly instead of kwargs
        if self.pipeline:
            if isinstance(self.pipeline, dlt.Pipeline):
                pipeline = self.pipeline
            else:
                raise TypeError(
                    f"DltDataser: `pipeline` argument should be of type `dlt.Pipeline` received {type(self.pipeline)} instead"
                )
        else:
            pipeline = dlt.pipeline(**self._get_kwargs())

        # if `combine` was used in `to.dlt(..., combine=base.DictResult())`, we save multiple
        # tables via the same pipeline
        if isinstance(data, dict):
            resources = [dlt.resource(value, name=name) for name, value in data.items()]
        else:
            resources = [dlt.resource([data], name=__name__)]

        load_info = pipeline.run(resources)
        return load_info.asdict()

    @classmethod
    def name(cls) -> str:
        return "dlt"


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [
        DltSaver,
    ]:
        registry.register_adapter(loader)


register_data_loaders()
