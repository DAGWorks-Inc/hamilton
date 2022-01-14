import logging
import typing
from types import ModuleType

import ray
import pandas as pd
import numpy as np


from . import node, base, driver

logger = logging.getLogger(__name__)


class RayExecutor(base.HamiltonExecutor, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Dask"""

    def __init__(self, result_builder: base.ResultMixin):
        self.result_builder = result_builder

    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        # NOTE: the type of ray  is unknown until they are computed
        if isinstance(input, ray._raylet.ObjectRef):
            return True
        elif node.type == pd.Series:
            return True
        elif node.type == np.array:
            return True

        return node.type == typing.Any or isinstance(input, node.type)

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return ray.remote(node.callable).remote(**kwargs)

    def build_result(self, **columns: typing.Dict[str, typing.Any]) -> typing.Any:
        for k, v in columns.items():
            logger.info(f'Got column {k}, with type [{type(v)}].')
        remote_combine = ray.remote(self.result_builder.build_result).remote(**columns)
        # object_refs = [remote_combine] + [v for v in columns.values() if isinstance(v, ray._raylet.ObjectRef)]
        # ready_refs, remaining_refs = ray.wait(object_refs, num_returns=len(object_refs), timeout=100)
        # print(f'Ready:', ready_refs)
        # print(f'Remaining: ', remaining_refs)

        df = ray.get(remote_combine)
        return df


class RayDriver(driver.Driver):

    def __init__(self,
                 DAG_config: typing.Dict[str, typing.Union[str, int, bool, float]],
                 initial_columns: typing.Dict[str, typing.Union[list, pd.Series, pd.DataFrame, np.ndarray]],
                 *modules: ModuleType,
                 ray_parameters: typing.Dict[str, typing.Any],
                 result_builder: base.ResultMixin):
        if ray_parameters.get('convert_to_dask_data_type', True):
            config = self.rayify_data(ray_parameters, initial_columns)
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
        super(RayDriver, self).__init__(config, *modules, executor=RayExecutor(result_builder))

    @staticmethod
    def rayify_data(ray_parameters, vector_config):
        """Makes things into ray data types."""
        # TODO:
        return vector_config

    def execute(self,
                final_vars: typing.List[str],
                overrides: typing.Dict[str, typing.Any] = None,
                display_graph: bool = False) -> pd.DataFrame:
        columns = self.raw_execute(final_vars, overrides, display_graph)
        return self.executor.build_result(**columns)



