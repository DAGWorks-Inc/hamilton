import abc
from typing import Any, List, Dict

import pandas as pd

"""Base classes for models in hamilton
We define a model as anything whose inputs are configuration-driven.
The goal of this is to allow users to write something like:

@model(...)
def my_column():
    #column that is some combinations of other columns using some previously trained model

Note that the model-training is not yet in scope."""


class BaseModel(abc.ABC):
    """Abstract class for a model as seen by hamilton"""

    def __init__(self, config_parameters: Any, name: str):
        self._config_parameters = config_parameters
        self._name = name

    @abc.abstractmethod
    def get_dependents(self) -> List[str]:
        """Gets the names/types of the inputs to this column.
         :return: A list of columns on which this model depends.
        """
        pass

    @abc.abstractmethod
    def predict(self, **columns: pd.Series) -> pd.Series:
        """Runs predictions based on a series of input values.
        Should probably operate on one row at a time so we don't have to worry about lookback.
        Alternatively, it could just output a scalar, E.G. the current cycle's value.
         :param columns: Columns that are inputs to the model
        :return: A series that is the results of the model's predictions
        """
        pass

    @property
    def config_parameters(self) -> Dict[str, Any]:
        """Accessor for configuration parameters"""
        return self._config_parameters

    @property
    def name(self) -> str:
        return self._name
