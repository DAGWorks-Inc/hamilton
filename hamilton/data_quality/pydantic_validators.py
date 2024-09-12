from typing import Any, Type

from pydantic import BaseModel, TypeAdapter, ValidationError

from hamilton.data_quality import base
from hamilton.htypes import custom_subclass_check


class PydanticModelValidator(base.BaseDefaultValidator):
    """Pydantic model compatibility validator

    Note that this validator uses pydantic's strict mode, which does not allow for
    coercion of data. This means that if an object does not exactly match the reference
    type, it will fail validation, regardless of whether it could be coerced into the
    correct type.

    :param model: Pydantic model to validate against
    :param importance: Importance of the validator, possible values "warn" and "fail"
    :param arbitrary_types_allowed: Whether arbitrary types are allowed in the model
    """

    def __init__(self, model: Type[BaseModel], importance: str):
        super(PydanticModelValidator, self).__init__(importance)
        self.model = model
        self._model_adapter = TypeAdapter(model)

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        # In addition to checking for a subclass of BaseModel, we also check for dict
        # as this is the standard 'de-serialized' format of pydantic models in python
        return custom_subclass_check(datatype, BaseModel) or custom_subclass_check(datatype, dict)

    def description(self) -> str:
        return "Validates that the returned object is compatible with the specified pydantic model"

    def validate(self, data: Any) -> base.ValidationResult:
        try:
            # Currently, validate can not alter the output data, so we must use
            # strict=True. The downside to this is that data that could be coerced
            # into the correct type will fail validation.
            self._model_adapter.validate_python(data, strict=True)
        except ValidationError as e:
            return base.ValidationResult(
                passes=False, message=str(e), diagnostics={"model_errors": e.errors()}
            )
        return base.ValidationResult(
            passes=True,
            message=f"Data passes pydantic check for model {str(self.model)}",
        )

    @classmethod
    def arg(cls) -> str:
        return "model"

    @classmethod
    def name(cls) -> str:
        return "pydantic_validator"


PYDANTIC_VALIDATORS = [PydanticModelValidator]
