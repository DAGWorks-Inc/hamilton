from typing import List

from pydantic import BaseModel

from hamilton import node
from hamilton.data_quality import base as dq_base
from hamilton.function_modifiers import InvalidDecoratorException
from hamilton.function_modifiers import base as fm_base
from hamilton.function_modifiers import check_output as base_check_output
from hamilton.function_modifiers.validation import BaseDataValidationDecorator
from hamilton.htypes import custom_subclass_check


class check_output(BaseDataValidationDecorator):
    def __init__(
        self,
        importance: str = dq_base.DataValidationLevel.WARN.value,
        target: fm_base.TargetType = None,
    ):
        super(check_output, self).__init__(target)
        self.importance = importance
        self.target = target

    def get_validators(self, node_to_validate: node.Node) -> List[dq_base.DataValidator]:
        output_type = node_to_validate.type
        if not custom_subclass_check(output_type, BaseModel):
            raise InvalidDecoratorException(
                f"Output of function {node_to_validate.name} must be a Pydantic model"
            )
        return base_check_output(
            importance=self.importance, model=output_type, target_=self.target
        ).get_validators(node_to_validate)
