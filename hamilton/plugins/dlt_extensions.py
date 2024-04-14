import dataclasses
from typing import Any, Collection, Dict, Iterable, Literal, Optional, Sequence, Tuple, Type

import dlt
import pandas as pd
from dlt.common.destination import Destination, TDestinationReferenceArg  # noqa: F401
from dlt.common.schema import Schema, TColumnSchema

# importing TDestinationReferenceArg fails if Destination isn't imported
from dlt.extract.resource import DltResource

from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataLoader, DataSaver

DATAFRAME_TYPES = [Iterable, pd.DataFrame]

# TODO add types for other Dataframe libraries
try:
    import pyarrow as pa

    DATAFRAME_TYPES.extend([pa.Table, pa.RecordBatch])
except ModuleNotFoundError:
    pass

# convert to tuple to dynamically define type `Union[DATAFRAME_TYPES]`
DATAFRAME_TYPES = tuple(DATAFRAME_TYPES)


# TODO use `driver.validate_materialization`
@dataclasses.dataclass
class DltResourceLoader(DataLoader):
    resource: DltResource

    @classmethod
    def name(cls) -> str:
        return "dlt"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return DATAFRAME_TYPES

    def load_data(self, type_: Type) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Creates a pipeline and conduct `extract` and `normalize` steps.
        Then, "load packages" are read with pandas
        """
        pipeline = dlt.pipeline(pipeline_name="Hamilton-DltResourceLoader")
        pipeline.extract(self.resource)
        normalize_info = pipeline.normalize(loader_file_format="parquet")

        partition_file_paths = []
        for package in normalize_info.load_packages:
            load_info = package.jobs["new_jobs"][0]
            partition_file_paths.append(load_info.file_path)

        # TODO use pyarrow directly to support different dataframe libraries
        # ref: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html#pyarrow.parquet.ParquetDataset
        df = pd.concat([pd.read_parquet(f) for f in partition_file_paths], ignore_index=True)

        # delete the pipeline
        pipeline.drop()

        metadata = utils.get_dataframe_metadata(df)
        return df, metadata


# TODO handle behavior with `combine=`, currently only supports materializing a single node
@dataclasses.dataclass
class DltDestinationSaver(DataSaver):
    """Materialize results using a dlt pipeline with the specified destination.

    In reference to an Extract, Transform, Load (ETL) pipeline, here, the Hamilton
    dataflow is responsible for Transform, and `DltDestination` for Load.
    """

    pipeline: dlt.Pipeline
    table_name: str
    primary_key: Optional[str] = None
    write_disposition: Optional[Literal["skip", "append", "replace", "merge"]] = None
    columns: Optional[Sequence[TColumnSchema]] = None
    schema: Optional[Schema] = None

    @classmethod
    def name(cls) -> str:
        return "dlt"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return DATAFRAME_TYPES

    def _get_kwargs(self) -> dict:
        kwargs = {}
        fields_to_skip = ["pipeline"]
        for field in dataclasses.fields(self):
            field_value = getattr(self, field.name)
            if field.name in fields_to_skip:
                continue

            if field_value != field.default:
                kwargs[field.name] = field_value

        return kwargs

    # TODO get pyarrow table from polars, dask, etc.
    def save_data(self, data) -> Dict[str, Any]:
        """
        ref: https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas
        """
        if isinstance(data, dict):
            raise NotImplementedError(
                "DltDestinationSaver received data of type `dict`."
                "Currently, it doesn't support specifying `combine=base.DictResult()`"
            )

        load_info = self.pipeline.run(data, **self._get_kwargs())
        return load_info.asdict()


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [
        DltDestinationSaver,
        DltResourceLoader,
    ]:
        registry.register_adapter(loader)


register_data_loaders()
