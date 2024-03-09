import functools
import subprocess

from hamilton.execution.executors import DefaultExecutionManager, TaskExecutor
from hamilton.execution.grouping import TaskImplementation
from hamilton.function_modifiers import tag


class CMDLineExecutionManager(DefaultExecutionManager):
    def get_executor_for_task(self, task: TaskImplementation) -> TaskExecutor:
        """Simple implementation that returns the local executor for single task executions,

        :param task: Task to get executor for
        :return: A local task if this is a "single-node" task, a remote task otherwise
        """
        is_single_node_task = len(task.nodes) == 1
        if not is_single_node_task:
            raise ValueError("Only single node tasks supported")
        (node,) = task.nodes
        if "cmdline" in node.tags:  # hard coded for now
            return self.remote_executor
        return self.local_executor


import inspect


def cmdline():
    """Decorator to run the result of a function as a command line command."""

    def decorator(func):
        if not inspect.isgeneratorfunction(func):
            raise ValueError("Function must be a generator.")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # we don't want to change the current working directory
            # until we are executing the function
            # name = func.__name__

            # If the function is a generator, we need to block and monitor the task if required
            generator = func(*args, **kwargs)
            cmd: str = next(generator)
            # Run the command and capture the output
            process = subprocess.run(cmd, shell=True, capture_output=True, check=True)

            try:
                process_result = process.stdout.decode("utf-8")
                generator.send(process_result)
                # ValueError should not be hit because a StopIteration should be raised, unless
                # there are multiple yields in the generator.
                raise ValueError("Generator cannot have multiple yields.")
            except StopIteration as e:
                result = e.value

            # change back to the original working directory
            # Return the output
            return result

        # get the return type and set it as the return type of the wrapper
        wrapper.__annotations__["return"] = inspect.signature(
            func
        ).return_annotation  # Jichen: why [2] ?
        return wrapper

    # decorator = tag(cmdline="slurm")(decorator)
    return decorator