import abc
import dataclasses
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List

from hamilton import node
from hamilton.execution.graph_functions import execute_subdag
from hamilton.execution.grouping import NodeGroupPurpose, TaskImplementation
from hamilton.execution.state import ExecutionState, TaskState

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class TaskFuture:
    """Simple representation of a future. TODO -- add cancel().
    This is a clean wrapper over a python future, and we may end up just using that at some point."""

    future: Any

    def get_state(self) -> TaskState:
        """Gets the state. This is non-blocking."""
        if self.future.done():
            try:
                self.future.result()
            except Exception:
                logger.exception("Task failed")
                return TaskState.FAILED
            return TaskState.SUCCESSFUL
        else:
            return TaskState.RUNNING

    def get_result(self) -> Any:
        """Gets the result. This is non-blocking.

        :return: None if there is no result, else the result
        """
        if not self.future.done():
            return None
        return self.future.result()


class TaskExecutor(abc.ABC):
    """Abstract class for a task executor. All this does is submit a task and return a future.
    It also tells us if it can do that"""

    @abc.abstractmethod
    def init(self):
        """Initializes the task executor, provisioning any necessary resources."""
        pass

    @abc.abstractmethod
    def finalize(self):
        """Tears down the task executor, freeing up any provisioned resources.
        Will be called in a finally block."""
        pass

    @abc.abstractmethod
    def submit_task(self, task: TaskImplementation) -> TaskFuture:
        """Submits a task to the executor. Returns a task ID that can be used to query the status.
        Effectively a future.

        :param task: Task implementation (bound with arguments) to submit
        :return: The future representing the task's computation.
        """
        pass

    @abc.abstractmethod
    def can_submit_task(self) -> bool:
        """Returns whether or not we can submit a task to the executor.
        For instance, if the maximum parallelism is reached, we may not be able to submit a task.

        TODO -- consider if this should be a "parallelism" value instead of a boolean, forcing
        the ExecutionState to store the state prior to executing a task.

        :return: whether or not we can submit a task.
        """
        pass


class SynchronousLocalTaskExecutor(TaskExecutor):
    """Basic synchronous/local task executor that runs tasks
    in the same process, at submit time."""

    def __init__(self):
        self.initialized = False

    def init(self):
        pass

    def finalize(self):
        pass

    def submit_task(self, task: TaskImplementation) -> TaskFuture:
        """Submitting a task is literally just running it.

        :param task: Task to submit
        :return: Future associated with this task
        """
        # No error management for now
        result = base_execute_task(task)
        return TaskFuture(future=None, get_state=lambda: TaskState.SUCCESSFUL, get_result=lambda: result)

    def can_submit_task(self) -> bool:
        """We can always submit a task as the task submission is blocking!

        :return: True
        """
        return True


class ThreadPoolTaskExecutor(TaskExecutor):
    """Task executor that runs tasks in a ThreadPoolExecutor."""

    def __init__(self, max_workers: int):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def init(self):
        pass

    def finalize(self):
        self.executor.shutdown()

    def submit_task(self, task: TaskImplementation) -> TaskFuture:
        """Submitting a task is running it in a thread from the pool.

        :param task: Task to submit
        :return: The future associated with the task
        """
        future = self.executor.submit(base_execute_task, task)
        return TaskFuture(future=future, get_state=lambda: TaskState.RUNNING, get_result=future.result)

    def can_submit_task(self) -> bool:
        # For ThreadPoolExecutor, we can always submit a task
        return True


class ExecutionManager(abc.ABC):
    """Manages execution per task. This enables you to have different executors for different
    tasks/task types. Note that, currently, it just uses the task information, but we could
    theoretically add metadata in a task as well.
    """

    @abc.abstractmethod
    def get_executor_for_task(self, task: TaskImplementation) -> TaskExecutor:
        """Selects the executor for the task. This enables us to set the appropriate executor
        for specific tasks (so that we can run some locally, some remotely, etc...).

        Note that this is the power-user case -- in all likelihood, people will use the default
        ExecutionManager.

        :param task:  Task to choose execution manager for
        :return: The executor to use for this task
        """
        pass


class DefaultExecutionManager(ExecutionManager):
    def __init__(self, local_executor: TaskExecutor, remote_executor: TaskExecutor):
        """Instantiates an ExecutionManager with a local/remote executor.
        These enable us to run certain tasks locally (simple transformations, generating sets of files),
        and certain tasks remotely (processing files in large datasets, etc...)

        :param local_executor: Executor to use for running tasks locally
        :param remote_executor:  Executor to use for running tasks remotely
        """
        self.local_executor = local_executor
        self.remote_executor = remote_executor

    def get_executor_for_task(self, task: TaskImplementation) -> TaskExecutor:
        """Simple implementation that returns the local executor for single task executions,
        and the remote executor for tasks to execute in a separate process.

        :param task: Task to get executor for
        :return: A local task executor if this is a "single-node" task, a remote task executor otherwise
        """
        if task.purpose == NodeGroupPurpose.EXECUTE_BLOCK:
            return self.remote_executor
        return self.local_executor


def run_graph_to_completion(
    execution_state: ExecutionState,
    execution_manager: ExecutionManager,
):
    """Blocking call to run the graph until it is complete. This employs a while loop.

    :return: Nothing, the execution state/result cache can give us the data
    """
    task_futures = {}
    try:
        while not GraphState.is_terminal(execution_state.get_graph_state()):
            # get the next task from the queue
            next_task = execution_state.release_next_task()
            if next_task is not None:
                task_executor = execution_manager.get_executor_for_task(next_task)
                if task_executor.can_submit_task():
                    try:
                        submitted = task_executor.submit_task(next_task)
                    except Exception as e:
                        logger.exception(
                            f"Exception submitting task {next_task.task_id}, with nodes: "
                            f"{[item.name for item in next_task.nodes]}"
                        )
                        raise e
                    task_futures[next_task.task_id] = submitted
                else:
                    # Whoops, back on the queue
                    # We should probably wait a bit here, but for now we're going to keep
                    # burning through
                    execution_state.reject_task(task_to_reject=next_task)
            # update all the tasks in flight
            # copy so we can modify
            for task_name, task_future in task_futures.copy().items():
                state = task_future.get_state()
                result = task_future.get_result()
                execution_state.update_task_state(task_name, state, result)
                if TaskState.is_terminal(state):
                    del task_futures[task_name]
        logger.info(f"Graph is done, graph state is {execution_state.get_graph_state()}")
    finally:
        # Ensure proper cleanup
        for task_name, task_future in task_futures.items():
            if not task_future.future.done():
                # Cancel any remaining tasks
                task_future.future.cancel()


# Utility function to execute a base task
def base_execute_task(task: TaskImplementation) -> Dict[str, Any]:
    """This is a utility function to execute a base task.

    :param task: task to execute.
    :return: a dictionary of the results of all the nodes in that task's nodes to compute.
    """
    for node_ in task.nodes:
        if not getattr(node_, "callable_modified", False):
            node_._callable = _modify_callable(node_.node_role, node_.callable)
        setattr(node_, "callable_modified", True)
    return execute_subdag(
        nodes=task.nodes,
        inputs=task.dynamic_inputs,
        adapter=task.adapters[0],  # TODO -- wire through multiple graph adapters
        overrides={**task.dynamic_inputs, **task.overrides},
    )


def new_callable(*args, _callable=None, **kwargs):
    return list(_callable(*args, **kwargs))


def _modify_callable(node_source: node.NodeType, callabl: Callable):
    """This is a bit of a shortcut -- we modify the callable here as
    we want to allow `Parallelizable[]` nodes to return a generator

    :param node_source:
    :param callabl:
    :return:
    """
    if node_source == node.NodeType.EXPAND:
        return functools.partial(new_callable, _callable=callabl)
    return callabl


# Example usage
if __name__ == "__main__":
    local_executor = SynchronousLocalTaskExecutor()
    thread_pool_executor = ThreadPoolTaskExecutor(max_workers=5)

    execution_manager = DefaultExecutionManager(local_executor=local_executor, remote_executor=thread_pool_executor)

    # Create a dummy task to demonstrate task execution
    dummy_task = TaskImplementation(
        task_id="dummy_task",
        nodes=[],  # Add your nodes here
        dynamic_inputs={},  # Add your dynamic inputs here
        adapters=[],  # Add your adapters here
        purpose=NodeGroupPurpose.EXECUTE_BLOCK,  # Adjust the purpose as needed
    )

    execution_state = ExecutionState()  # Assuming you have an ExecutionState instance

    run_graph_to_completion(execution_state, execution_manager)  # Run the task graph to completion

