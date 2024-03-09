from .adapter import LocalAdapter, RemoteAdapter


class Submitor:
    """
    Base class for submitors.
    """
    def __init__(self, name:str, is_remote:bool = False):
        self.name = name
        self.script_name = "submit.sh"
        self.is_remote = is_remote
        if self.is_remote:
            raise NotImplementedError("Remote submitor is not implemented yet.")
            # self._adapter = RemoteAdapter()
        else:
            self._adapter = LocalAdapter()
    
    def _write_script(self, cmd:str, config:dict):

        with open(self.script_name, "w") as file:
            for key, value in config.items():
                file.write(f"{key}={value}\n")

            file.write(f"{cmd}\n")