from time import sleep

class Monitor:
    job_queue = []

    def __init__(self, submitor, job_id: int, user: str = None):
        self.submitor = submitor
        self.job_id = job_id
        self.job_queue.append(self)
        self.user = user

    def status(self):
        return self.submitor(self.job_id)

    def __del__(self):
        if self in self.job_queue:
            self.job_queue.remove(self)

    def __eq__(self, other: "Monitor"):
        return self.job_id == other.job_id

    def wait(self, interval=5):
        while True:
            status = self.status()
            if status == "completed":
                break
            elif status == "failed":
                raise ValueError(f"Job {self.job_id} failed.")
            else:
                print(f"Job {self.job_id} is {status}.")
            sleep(interval)

    @classmethod
    def get_all_job_status(cls):
        return [monitor.status() for monitor in cls.job_queue]

    @classmethod
    def wait_all(self, interval=5):
        while True:
            status = self.get_all_job_status()
            if all([s == "completed" for s in status]):
                break
            elif any([s == "failed" for s in status]):
                raise ValueError(f"Job {self.job_id} failed.")
            else:
                print(f"Jobs are {status}.")
            sleep(interval)
