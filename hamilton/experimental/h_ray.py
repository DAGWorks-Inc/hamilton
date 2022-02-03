import logging
import typing

import ray

from hamilton import base
from hamilton import node

logger = logging.getLogger(__name__)


class RayGraphAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Ray

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, result_builder: base.ResultMixin):
        self.result_builder = result_builder

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        # NOTE: the type of a raylet is unknown until they are computed
        if isinstance(input_value, ray._raylet.ObjectRef):
            return True
        return node_type == typing.Any or isinstance(input_value, node_type)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        return node_type == input_type

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
