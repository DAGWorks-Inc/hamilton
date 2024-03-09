from .submitor import Submitor
from .slurm import Slurm

class Cluster:
    """
    A class to manage different cluster submitors.
    """
    def __init__(self, ):
        
        self._cluster:dict[str, Submitor] = {}

    def init(self, name:str, type:str)->Submitor:

        if name in self._cluster:
            return self._cluster[name]
        else:
            if type == "slurm":
                self._cluster[name] = Slurm(name)
            else:
                raise ValueError(f"Cluster type {type} not supported.")
            
        return self._cluster[name]
    