from abc import ABC, abstractmethod
from pathlib import Path
from subprocess import check_output


class Adapter(ABC):

    @abstractmethod
    def execute(self, cmd: str, work_dir: str | Path | None = None):
        pass


class LocalAdapter(Adapter):

    def execute(self, cmd: str, work_dir: str | Path | None = None):
        out = check_output(cmd, cwd=work_dir, shell=True, universal_newlines=True)
        return out


class RemoteAdapter(Adapter):
    pass
