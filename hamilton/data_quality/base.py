import abc
import dataclasses
import enum
import logging
import typing
from typing import Type, Any, List, Dict

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    pass


class DataValidationLevel(enum.Enum):
    WARN = 'warn'
    FAIL = 'fail'


@dataclasses.dataclass
class ValidationResult:
    passes: bool  # Whether or not this passed the validation
    message: str  # Error message or success message
    diagnostics: Dict[str, Any] = dataclasses.field(default_factory=dict)  # Any extra diagnostics information needed, free-form


class DataValidator(abc.ABC):
    """Base class for a data quality operator. This will be used by the `data_quality` operator"""

    def __init__(self, importance: str):
        self._importance = DataValidationLevel(importance)

    @property
    def importance(self) -> DataValidationLevel:
        return self._importance

    @abc.abstractmethod
    def applies_to(self, datatype: Type[Type]) -> bool:
        """Whether or not this data validator can apply to the specified dataset

        :param datatype:
        :return: True if it can be run on the specified type, false otherwise
        """
        pass

    @abc.abstractmethod
    def description(self) -> str:
        """Gives a description of this validator. E.G.
        `Checks whether the entire dataset lies between 0 and 1.`
        Note it should be able to access internal state (E.G. constructor arguments).
        :return: The description of the validator as a string
        """
        pass

    @classmethod
    @abc.abstractmethod
    def name(cls) -> str:
        """Returns the name for this validator."""

    @abc.abstractmethod
    def validate(self, dataset: Any) -> ValidationResult:
        """Actually performs the validation. Note when you

        :param dataset: dataset to validate
        :return: The result of validation
        """
        pass


def _act_warn(validation_result: ValidationResult, validator: DataValidator):
    if not validation_result.passes:
        logger.warning(f'Validator: {validator.name()} failed. Message was: {validation_result.message}. '
                       f'Diagnostic information is: {validation_result.diagnostics}')


def _act_fail(validation_result: ValidationResult, validator: DataValidator):
    if not validation_result.passes:
        raise DataValidationError(f'Validator: {validator.name()} failed. Message was: {validation_result.message}. '
                                  f'Diagnostic information is: {validation_result.diagnostics}')


def act(validation_result: ValidationResult, validator: DataValidator):
    """This is the current default for acting on the validation result.
    Note that we might move this at some point -- we'll want to make it configurable. But for now, this
    seems like a fine place to put it.

    @return:
    """
    if validator.importance == DataValidationLevel.WARN:
        _act_warn(validation_result, validator)
    elif validator.importance == DataValidationLevel.FAIL:
        _act_fail(validation_result, validator)


class BaseDefaultValidator(DataValidator, abc.ABC):
    """Base class for a default validator.
    These are all validators that utilize a single argument to be passed to the decorator check_output.
    check_output can thus delegate to multiple of these. This is an internal abstraction to allow for easy
    creation of validators.
    """

    def __init__(self, importance: str):
        super(BaseDefaultValidator, self).__init__(importance)

    @classmethod
    @abc.abstractmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        pass

    @abc.abstractmethod
    def description(self) -> str:
        pass

    @abc.abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        pass

    @classmethod
    @abc.abstractmethod
    def arg(cls) -> str:
        """Yields a string that represents this validator's argument.
        @check_output() will be passed a series of kwargs, each one of which will correspond to
        one of these default validators. Note that we have the limitation of allowing just a single
        argument.

        :return: The argument that this needs.
        """
        pass


class DataProfiler(abc.ABC):

    @classmethod
    @abc.abstractmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        """Does this profile handle the datatype specified?

        @param datatype: Data type it could handle
        @return: Whether or not it handles that datatype
        """
        pass

    @classmethod
    @abc.abstractmethod
    def profile_type(cls) -> Type[Type]:
        """Type that this profiler returns. Will usually be something internal.
        That said, this could be any metric if we want to split into multiple.

        @return:
        """
        pass

    @abc.abstractmethod
    def description(self) -> str:
        """Gives description for what this does

        @return: The description of this profiler.
        """
        pass

    @abc.abstractmethod
    def profile(self, data: Any) -> Any:
        """Profiles the data

        @param data: Data to profile
        @return: A profiling result
        """

