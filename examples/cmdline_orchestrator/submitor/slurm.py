from pathlib import Path
from .submitor import Submitor

def parse_submit_output(output:str) -> int:
    return int(output.splitlines()[-1])

def parse_queue_output(output:str) -> str:
    line_split_lst = [line.split("|") for line in output.splitlines()]
    if len(line_split_lst) != 0:
        job_id_lst, user_lst, status_lst, job_name_lst, working_directory_lst = zip(
            *[
                (int(jobid), user, status.lower(), jobname, working_directory)
                for jobid, user, status, jobname, working_directory in line_split_lst
            ]
        )
    else:
        job_id_lst, user_lst, status_lst, job_name_lst, working_directory_lst = (
            [],
            [],
            [],
            [],
            [],
        )
    df = {
            "jobid": job_id_lst,
            "user": user_lst,
            "jobname": job_name_lst,
            "status": status_lst,
            "working_directory": working_directory_lst,
        }
    
    if df["status"] == "r":
        df["status"] = "running"
    if df["status"] == "pd":
        df["status"] = "pending"

    return df

class Slurm(Submitor):

    def __init__(self, name:str, is_remote:bool = False):
        
        super().__init__(name, is_remote)

    def submit(self, cmd:str, job_name:str, n_cores:int, memory:int|None, max_runtime:int|None, work_dir:str|Path|None, **others:dict)->int:
        
        config = {
            "--job-name": job_name,
            "--cpus-per-task": n_cores,
        }
        if memory:
            config["--mem"] = memory
        if max_runtime:
            config["--time"] = max_runtime
        if work_dir:
            config["--chdir"] = work_dir

        if others:
            config.update(others)

        config = {f"#SBATCH {key}": value for key, value in config.items()}

        self._write_script(cmd, config)
        cmd = f"sbatch {self.script_name}"
        output = self._adapter.execute(cmd)
        job_id = parse_submit_output(output)
        return job_id

    def delete(self, job_id:int):
        cmd = f"scancel {job_id}"
        output = self._adapter.execute(cmd)

    def status(self, job_id:int):
        cmd = f"squeue --format %A|%u|%t|%.15j|%Z --noheader {job_id}"
        output = self._adapter.execute(cmd)
        status = parse_queue_output(output)
        return status