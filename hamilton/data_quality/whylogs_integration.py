import dataclasses
from typing import Type, List, Any

from hamilton import function_modifiers, node
from hamilton.data_quality import base
from hamilton.data_quality.base import ValidationResult

"""This is a POC for what a more complex whylogs integration might look like"""


@dataclasses.dataclass
class WhyLogsDataProfile:
    """Placeholder for a dataclass that represents a data profile for whylogs.
    We can replace this with whatever the whylogs client likes having, or keep
    a custom representation.
    """
    pass

class WhyLogsValidator(base.DataValidator):
    def __init__(self, condition: str):
        self.condition = condition

    def applies_to(self, datatype: Type[Type]) -> bool:
        return issubclass(datatype, WhyLogsDataProfile)

    def description(self) -> str:
        return f"Validates profile against condition: {self.condition}"

    @classmethod
    def name(cls) -> str:
        return "whylogs_validator"

    def validate(self, dataset: Any) -> ValidationResult:
        raise ValueError(f"Implement me -- run the condition...")

class whylogs_validate(function_modifiers.BaseDataValidationDecorator):
    def __init__(self):
        super(whylogs_validate, self).__init__()

    def get_validators(self, node_to_validate: node.Node) -> List[base.DataValidator]:
        pass

    def profile(self, data: Any) -> Any:
        raise ValueError(f"I should be calling out to the whylogs API here. Implement me!")

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        raise ValueError(f"Which datatypes can whylogs work with? Spark, pandas?")

    @classmethod
    def profile_type(cls) -> Type[Type]:
        return WhyLogsDataProfile

    def description(self) -> str:
        return "Class that profiles data in the whylogs shape."
