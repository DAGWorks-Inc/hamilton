import abc
import numbers
from typing import Any, Type, List, Optional

from hamilton.data_quality.base import DataValidator, ValidationResult
import pandas as pd


class BaseDefaultValidator(DataValidator, abc.ABC):
    """Base class for a default validator.
    These are all validators that utilize a single argument to be passed to the decorator check_output.
    check_output can thus delegate to multiple of these. This is an internal abstraction to allow for easy
    creation of validators.
    """

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

    @staticmethod
    def resolve(arg: str, type_: Type[Type], candidates: Optional[List[Type['BaseDefaultValidator']]] = None) -> Type['BaseDefaultValidator']:
        """Resolves a validator given two things:
        1. An argument
        2. A type to which it should apply

        :param arg: The argument that we want to evaluate
        :param type_: The type that we need it to apply to
        :param candidates: The set of candidates that we have to choose from
        :return:
        """
        if candidates is None:
            candidates = AVAILABLE_DEFAULT_VALIDATORS

        for candidate in candidates:
            if arg == candidate.arg() and candidate.applies_to(type_):
                return candidate
        raise ValueError(f'No registered subclass of BaseDefaultValidator is available '
                         f'for arg: {arg} and type {type_}. This either means (a) this arg-type '
                         f"contribution isn't supported or (b) this has not been added yet (but should be). "
                         f'In the case of (b), we welcome contributions. Get started at github.com/stitchfix/hamilton')


class DataInRangeValidatorPandas(BaseDefaultValidator):

    def __init__(self, range: str):
        """Data validator that tells if data is in a range. This applies to primitives (ints, floats).

        :param range: Inclusive range of parameters
        """
        self.range = range

    @classmethod
    def arg(cls) -> str:
        return 'range'

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, pd.Series)  # TODO -- handle dataframes?

    def description(self) -> str:
        return f'Validates that the datapoint falls within the range ({self.range[0]}, {self.range[1]})'

    def validate(self, data: pd.Series) -> ValidationResult:
        min_, max_ = self.range
        between = data.between(min_, max_, inclusive=True)
        counts = between.value_counts()
        in_range = counts[True]
        out_range = counts[False]
        passes = out_range == 0
        message = f'Series contains {in_range} values in range ({min_},{max_}), and {out_range} outside.'
        return ValidationResult(
            passes=passes,
            message=message,
            diagnostics={
                'range': self.range,
                'in_range': in_range,
                'out_range': out_range,
                'data_size': len(data)
            }
        )


class DataInRangeValidatorPrimitives(BaseDefaultValidator):
    def __init__(self, range: str):
        """Data validator that tells if data is in a range. This applies to primitives (ints, floats).

        :param range: Inclusive range of parameters
        """
        self.range = range

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, numbers.Real)

    def description(self) -> str:
        return f'Validates that the datapoint falls within the range ({self.range[0]}, {self.range[1]})'

    def validate(self, data: numbers.Real) -> ValidationResult:
        min_, max_ = self.range
        passes = min_ <= data <= max_
        message = f'Data point {data} falls within acceptable range: ({min_}, {max_})' if passes else \
            f'Data point {data} does not fall within acceptable range: ({min_}, {max_})'
        return ValidationResult(
            passes=passes,
            message=message,
            diagnostics={
                'range': self.range,
                'value': data
            }
        )

    @classmethod
    def arg(cls) -> str:
        return 'range'


AVAILABLE_DEFAULT_VALIDATORS = [
    DataInRangeValidatorPandas,
    DataInRangeValidatorPrimitives,
]

def resolve_validators(output_type: Type[Type], **default_validator_kwargs) -> List[BaseDefaultValidator]:
    validators = []
    for key in default_validator_kwargs.keys():
        for validator_cls in AVAILABLE_DEFAULT_VALIDATORS:
            if validator_cls.arg() == key and validator_cls.applies_to(output_type):
                output.append(validator_cls(**default_validator_kwargs))
                break
    return validators