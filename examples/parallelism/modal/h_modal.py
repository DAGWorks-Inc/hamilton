from types import FunctionType
from typing import Any, Callable, Dict

import modal

from hamilton.execution import executors
from hamilton.execution.executors import (
    ExecutionManager,
    SynchronousLocalTaskExecutor,
    TaskExecutor,
    TaskFuture,
    base_execute_task,
)
from hamilton.execution.grouping import TaskImplementation
from hamilton.execution.state import TaskState
from hamilton.function_modifiers import tag

MODAL = "modal"


#
# stub = modal.Stub()

#
# @stub.function()
# def globally_scoped(a: int):
#     return 1
#
#     # return base_execute_task(task)
#
# class TaskExecutor:
#     def __init__(self, task: TaskImplementation):
#         self.task = task
#
#     def __enter__(self):
#         pass
#
#     @method()
#     def run(self):
#         return base_execute_task(self.task)
#


class ModalExecutor(executors.TaskExecutor):
    def __init__(self, stub_params: Dict[str, Any], global_function_params: Dict[str, Any] = None):
        self.stub = modal.Stub(**stub_params)
        self.run = self.stub.run()
        self.global_function_params = global_function_params or {}

    def init(self):
        """Initializes the modal executor, by entering the context manager.
        TODO -- be more specific about this -- we could enter and exit multiple times?
        """

    def finalize(self):
        """Finalizes the modal executor, by ending the context manager.

        @return:
        """
        # self.run.__exit__(None, None, None)

    def submit_task(self, task: TaskImplementation) -> TaskFuture:
        function_params = dict(name=task.task_id, serialized=True, **self.global_function_params)
        function_to_submit = self.stub.function(**function_params)(base_execute_task)

        with self.stub.run():
            result = function_to_submit.remote(task=task)
            return TaskFuture(get_state=lambda: TaskState.SUCCESSFUL, get_result=lambda: result)

    def can_submit_task(self) -> bool:
        return True


class RemoteExecutionManager(ExecutionManager):
    TAG_KEY = "remote"

    def __init__(self, **remote_executors: TaskExecutor):
        self.local_executor = SynchronousLocalTaskExecutor()
        self.remote_executors = remote_executors
        super(RemoteExecutionManager, self).__init__(
            [self.local_executor] + list(remote_executors.values())
        )

    def get_executor_for_task(self, task: TaskImplementation) -> TaskExecutor:
        """Simple implementation that returns the local executor for single task executions,

        :param task: Task to get executor for
        :return: A local task if this is a "single-node" task, a remote task otherwise
        """
        is_single_node_task = len(task.nodes) == 1
        if not is_single_node_task:
            raise ValueError("Only single node tasks supported")
        (node,) = task.nodes
        tag_value = node.tags.get(self.TAG_KEY)
        if tag_value is None:
            return self.local_executor
        if tag_value in self.remote_executors:
            return self.remote_executors[tag_value]
        raise ValueError(f"Unknown remote source {tag_value}")


# class ModalExecutionManager(DelegatingExecutionManager):
#     def __init__(self, stub_params: Dict[str, Any], global_function_params: Dict[str, Any] = None):
#         remote_executor = ModalExecutor(stub_params)
#         super().__init__(remote_executor, remote_tag_value=MODAL)


def remote(source: str) -> Callable[[FunctionType], FunctionType]:
    def decorator(fn: FunctionType) -> FunctionType:
        return tag(remote=source)(fn)

    return decorator
