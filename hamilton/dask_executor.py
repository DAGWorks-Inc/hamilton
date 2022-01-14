import logging
import typing
from types import ModuleType

from dask import compute
from dask.distributed import Client as DaskClient
from dask.delayed import Delayed, delayed
import dask.dataframe
import dask.array
import pandas as pd
import numpy as np


from . import node, base, driver

logger = logging.getLogger(__name__)


class DaskExecutor(base.HamiltonExecutor, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Dask"""

    def __init__(self, dask_client: DaskClient, result_builder: base.ResultMixin, visualize: bool = True):
        self.client = dask_client
        self.result_builder = result_builder
        self.visualize = visualize

    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        # NOTE: the type of dask Delayed is unknown until they are computed
        if isinstance(input, Delayed):
            return True
        elif node.type == pd.Series and isinstance(input, dask.dataframe.Series):
            return True
        elif node.type == np.array and isinstance(input, dask.array.Array):
            return True

        return node.type == typing.Any or isinstance(input, node.type)

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return delayed(node.callable)(**kwargs)

    def build_result(self, **columns: typing.Dict[str, typing.Any]) -> typing.Any:
        for k, v in columns.items():
            logger.info(f'Got column {k}, with type [{type(v)}].')
        delayed_combine = delayed(self.result_builder.build_result)(**columns)
        # columns, = compute(columns)
        # return pd.DataFrame(columns)
        if self.visualize:
            delayed_combine.visualize()
        df, = compute(delayed_combine)
        return df


class DaskDriver(driver.Driver):

    def __init__(self,
                 DAG_config: typing.Dict[str, typing.Union[str, int, bool, float]],
                 initial_columns: typing.Dict[str, typing.Union[list, pd.Series, pd.DataFrame, np.ndarray]],
                 *modules: ModuleType,
                 dask_client: DaskClient,
                 dask_parameters: typing.Dict[str, typing.Any],
                 result_builder: base.ResultMixin):
        self.client = dask_client
        if dask_parameters.get('convert_to_dask_data_type', True):
            config = self.daskify_data(dask_parameters, initial_columns)
        else:
            config = initial_columns.copy()  # shallow copy
        config: typing.Dict[str, typing.Any]  # can now become anything...
        errors = []
        for scalar_key in DAG_config.keys():
            if scalar_key in config:
                errors.append(f'\nError: {scalar_key} already defined in vector_config.')
            else:
                config[scalar_key] = DAG_config[scalar_key]
        if errors:
            raise ValueError(f'Bad configs passed:\n [{"".join(errors)}].')
        super(DaskDriver, self).__init__(config, *modules, executor=DaskExecutor(dask_client, result_builder))

    @staticmethod
    def daskify_data(dask_parameters, vector_config):
        """Makes things into dask data types."""
        config = {}
        default_chunksize = dask_parameters.get('default_chunksize')
        default_npartitions = dask_parameters.get('default_npartitions')
        data_config = dask_parameters.get('data_config', {})
        for col, value in vector_config.items():
            if isinstance(value, (pd.Series, pd.DataFrame)):
                chunksize = data_config.get(f'{col}.chunksize', default_chunksize)
                npartitions = data_config.get(f'{col}.npartitions', default_npartitions)
                if chunksize and npartitions:
                    chunksize = None  # only want one of them -- default to npartitions.
                config[col] = dask.dataframe.from_pandas(
                    value, name=col, chunksize=chunksize, npartitions=npartitions)
            elif isinstance(value, list) or isinstance(value, np.ndarray):
                config[col] = dask.array.from_array(value, name=col)
            else:
                raise ValueError(f'Do not know how to convert {col}:{type(value)} to dask data type.')
        return config

    def execute(self,
                final_vars: typing.List[str],
                overrides: typing.Dict[str, typing.Any] = None,
                display_graph: bool = False) -> pd.DataFrame:
        columns = self.raw_execute(final_vars, overrides, display_graph)
        return self.executor.build_result(**columns)



