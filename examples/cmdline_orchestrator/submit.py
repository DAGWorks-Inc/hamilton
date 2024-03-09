import inspect, functools
from submitor.cluster import Cluster
from submitor.monitor import Monitor
from hamilton.function_modifiers import tag

cluster = Cluster()

def submit(name:str, type:str):
    """Decorator to run the result of a function as a command line command."""

    submitor = cluster.init(name, type)

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
            arguments: dict = next(generator)
            monitor = arguments.pop("monitor", False)
            cmd = arguments.pop("cmd")
            # Run the command and capture the output
            job_id = submitor.submit(cmd, **arguments)
            # slurm_task_info := Submitted batch job 30588834

            # TODO: blocked, but usually many workflow execute in parallel,
            #  so we only need a global monitor to poll all tasks' statuses
            if monitor:
                monitor = Monitor(monitor, job_id)
                monitor.wait()
            try:
                process_result = {"job_id": job_id}
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

    decorator = tag(cmdline="slurm")(decorator)
    return decorator