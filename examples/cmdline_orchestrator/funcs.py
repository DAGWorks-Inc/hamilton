import time
from subprocess import CompletedProcess

from cmdline import cmdline
from submit import submit

from hamilton.function_modifiers import tag

@cmdline()
def echo(message: str) -> str:
    # before
    result = yield f"echo {message}"
    # after
    print(result)
    return result

@submit(name="local_slurm", type="slurm")  # Jichen: initialize new submitor
def echo_slurm(message: str) -> str:

    # before
    result = yield {
        "job_name": "test_slurm1",
        "n_cores": 4,
        "memory": 1000,
        "max_runtime": 100,
        "work_dir": ".",
        "monitor": True,
        "cmd": f"echo {message}"
    }
    # after
    return "done"

@submit(name="local_slurm")  # Jichen: reuse submitor
def echo_slurm2(message: str) -> str:

    # before
    result = yield {
        "job_name": "test_slurm2",
        "n_cores": 4,
        "memory": 1000,
        "max_runtime": 100,
        "work_dir": ".",
        "monitor": True,
        "cmd": f"echo {message}"
    }
    # after
    return "done"

# @submit(name="local_lsf", type="lsf")  # Jichen: another submitor
# def echo_lsf(message: str) -> str: ...