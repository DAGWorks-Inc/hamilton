import logging
import typing

import ray

from hamilton import base
from hamilton import node

logger = logging.getLogger(__name__)


class RayGraphAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Ray

    Use `pip install sf-hamilton[ray]` to get the dependencies required to run this.

    Ray is a quick choice to scale computation for any type of Hamilton graph.

    # Notes on scaling:
      - Multi-core on single machine ✅
      - Distributed computation on a Ray cluster ✅
      - Scales to any size of data ⛔️; you are LIMITED by the memory on the instance/computer 💻.

    # Function return object types supported:
     - Works for any python object that can be serialized by the Ray framework. ✅

    # Pandas?
     - ⛔️ Ray DOES NOT do anything special about Pandas.

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, result_builder: base.ResultMixin):
        """Constructor

        :param result_builder: Required. An implementation of base.ResultMixin.
        """
        self.result_builder = result_builder
        if not self.result_builder:
            raise ValueError('Error: ResultMixin object required. Please pass one in for `result_builder`.')

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
        """Function that is called as we walk the graph to determine how to execute a hamilton function.

        :param node: the node from the graph.
        :param kwargs: the arguments that should be passed to it.
        :return: returns a ray object reference.
        """
        return ray.remote(node.callable).remote(**kwargs)

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[ray object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        if logger.isEnabledFor(logging.DEBUG):
            for k, v in outputs.items():
                logger.debug(f'Got column {k}, with type [{type(v)}].')
        # need to wrap our result builder in a remote call and then pass in what we want to build from.
        remote_combine = ray.remote(self.result_builder.build_result).remote(**outputs)
        result = ray.get(remote_combine)  # this materializes the object locally
        return result
