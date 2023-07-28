import functools
import logging
import typing

import ray
from ray import workflow

from hamilton import base, node
from hamilton.base import SimplePythonGraphAdapter
from hamilton.execution import executors
from hamilton.execution.executors import TaskFuture
from hamilton.execution.grouping import TaskImplementation

logger = logging.getLogger(__name__)


def raify(fn):
    """Makes the function into something ray-friendly.
    This is necessary due to https://github.com/ray-project/ray/issues/28146.

    :param fn: Function to make ray-friendly
    :return: The ray-friendly version
    """
    if isinstance(fn, functools.partial):

        def new_fn(*args, **kwargs):
            return fn(*args, **kwargs)

        return new_fn
    return fn


class RayGraphAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Ray.

    This walks the graph and translates it to run onto `Ray <https://ray.io/>`__.

    Use `pip install sf-hamilton[ray]` to get the dependencies required to run this.

    Use this if:

      * you want to utilize multiple cores on a single machine, or you want to scale to larger data set sizes with\
        a Ray cluster that you can connect to. Note (1): you are still constrained by machine memory size with Ray; you\
        can't just scale to any dataset size. Note (2): serialization costs can outweigh the benefits of parallelism \
        so you should benchmark your code to see if it's worth it.

    Notes on scaling:
    -----------------
      - Multi-core on single machine ✅
      - Distributed computation on a Ray cluster ✅
      - Scales to any size of data ⛔️; you are LIMITED by the memory on the instance/computer 💻.

    Function return object types supported:
    ---------------------------------------
      - Works for any python object that can be serialized by the Ray framework. ✅

    Pandas?
    --------
      - ⛔️ Ray DOES NOT do anything special about Pandas.

    CAVEATS
    -------
      - Serialization costs can outweigh the benefits of parallelism, so you should benchmark your code to see if it's\
      worth it.

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, result_builder: base.ResultMixin):
        """Constructor

        You have the ability to pass in a ResultMixin object to the constructor to control the return type that gets \
        produce by running on Ray.

        :param result_builder: Required. An implementation of base.ResultMixin.
        """
        self.result_builder = result_builder
        if not self.result_builder:
            raise ValueError(
                "Error: ResultMixin object required. Please pass one in for `result_builder`."
            )

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        # NOTE: the type of a raylet is unknown until they are computed
        if isinstance(input_value, ray._raylet.ObjectRef):
            return True
        return SimplePythonGraphAdapter.check_input_type(node_type, input_value)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Function that is called as we walk the graph to determine how to execute a hamilton function.

        :param node: the node from the graph.
        :param kwargs: the arguments that should be passed to it.
        :return: returns a ray object reference.
        """
        return ray.remote(raify(node.callable)).remote(**kwargs)

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[ray object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        if logger.isEnabledFor(logging.DEBUG):
            for k, v in outputs.items():
                logger.debug(f"Got output {k}, with type [{type(v)}].")
        # need to wrap our result builder in a remote call and then pass in what we want to build from.
        remote_combine = ray.remote(self.result_builder.build_result).remote(**outputs)
        result = ray.get(remote_combine)  # this materializes the object locally
        return result


class RayWorkflowGraphAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's required to make Hamilton run Ray Workflows

    Use `pip install sf-hamilton[ray]` to get the dependencies required to run this.

    Ray workflows is a more robust way to scale computation for any type of Hamilton graph.

    What's the difference between this and RayGraphAdapter?
    --------------------------------------------------------
        * Ray workflows offer durable computation. That is, they save and checkpoint each function.
        * This enables one to run a workflow, and not have to restart it if something fails, assuming correct\
        Ray workflow usage.

    Tips
    ----
    See https://docs.ray.io/en/latest/workflows/basics.html for the source of the following:

        1. Functions should be idempotent.
        2. The workflow ID is what Ray uses to try to resume/restart if run a second time.
        3. Nothing is run until the entire DAG is walked and setup and build_result is called.

    Notes on scaling:
    -----------------
      - Multi-core on single machine ✅
      - Distributed computation on a Ray cluster ✅
      - Scales to any size of data ⛔️; you are LIMITED by the memory on the instance/computer 💻.

    Function return object types supported:
    ---------------------------------------
      - Works for any python object that can be serialized by the Ray framework. ✅

    Pandas?
    --------
      - ⛔️ Ray DOES NOT do anything special about Pandas.

    CAVEATS
    -------
      - Serialization costs can outweigh the benefits of parallelism, so you should benchmark your code to see if it's\
      worth it.

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, result_builder: base.ResultMixin, workflow_id: str):
        """Constructor

        :param result_builder: Required. An implementation of base.ResultMixin.
        :param workflow_id: Required. An ID to give the ray workflow to identify it for durability purposes.
        :param max_retries: Optional. The function will be retried for the given number of times if an
            exception is raised.
        """
        self.result_builder = result_builder
        self.workflow_id = workflow_id
        if not self.result_builder:
            raise ValueError(
                "Error: ResultMixin object required. Please pass one in for `result_builder`."
            )

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        # NOTE: the type of a raylet is unknown until they are computed
        if isinstance(input_value, ray._raylet.ObjectRef):
            return True
        return SimplePythonGraphAdapter.check_input_type(node_type, input_value)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return ray.remote(raify(node.callable)).bind(**kwargs)

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[ray object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        if logger.isEnabledFor(logging.DEBUG):
            for k, v in outputs.items():
                logger.debug(f"Got output {k}, with type [{type(v)}].")
        # need to wrap our result builder in a remote call and then pass in what we want to build from.
        remote_combine = ray.remote(self.result_builder.build_result).bind(**outputs)
        result = workflow.run(
            remote_combine, workflow_id=self.workflow_id
        )  # this materializes the object locally
        return result


class RayTaskExecutor(executors.TaskExecutor):
    """Task executor using Ray for the new task-based execution mechanism in Hamilton.
    This is still experimental, so the API might change.
    """

    def __init__(self, num_cpus: int):
        """Creates a ray task executor. Note this will likely take in more parameters. This is
        experimental, so the API will likely change, although we will do our best to make it
        backwards compatible.

        :param num_cpus: Number of cores to use for initialization, passed drirectly to ray.init
        """
        self.num_cpus = num_cpus

    def init(self):
        ray.init(num_cpus=self.num_cpus)

    def finalize(self):
        ray.shutdown()

    def submit_task(self, task: TaskImplementation) -> TaskFuture:
        """Submits a task, wrapping it in a TaskFuture (after getting the corresponding python
        future).

        :param task: Task to wrap
        :return: A future
        """

        return executors.TaskFutureWrappingPythonFuture(
            ray.remote(executors.base_execute_task).remote(task=task).future()
        )

    def can_submit_task(self) -> bool:
        """For now we can always submit a task -- it might just be delayed.

        :return: True
        """
        return True
