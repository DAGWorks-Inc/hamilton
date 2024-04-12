import dataclasses
from typing import Any, Collection, Dict, Iterable, Literal, Optional, Type

import dlt
import pandas as pd

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

    pipeline: dlt.Pipeline
    table_name: Optional[dict] = None
    # kwargs for pipeline.run()
    write_disposition: Optional[Literal["skip", "append", "replace", "merge"]] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return SAVER_TYPES

    def save_data(self, data) -> Dict[str, Any]:
        """
        ref:
            tables: https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas
            standalone resources: https://dlthub.com/docs/general-usage/resource#load-resources
            dynamic resources: https://dlthub.com/docs/general-usage/source#create-resources-dynamically
        """
        if isinstance(data, dict) is False:  # only 1 node, no `combine=base.DictResult()`
            raise NotImplementedError(
                f"DltSaver needs to a receive a `Dict[str, {SAVER_TYPES}]`"
                f"When using `to.dlt()`, make sure to specify `combine=base.DictResult()`"
            )

        if self.table_name is None:
            self.table_name = dict()

        resources = []
        for node_name, result in data.items():
            name = self.table_name.get(node_name, node_name)
            resources.append(dlt.resource(result, name=name))

        load_info = self.pipeline.run(resources, write_disposition=self.write_disposition)
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
