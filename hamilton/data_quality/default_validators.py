import logging
import numbers
from typing import Type, List, Tuple

import numpy as np
import pandas as pd

from hamilton.data_quality import base
from hamilton.data_quality.base import BaseDefaultValidator

logger = logging.getLogger(__name__)


class DataInRangeValidatorPandas(BaseDefaultValidator):

    def __init__(self, range: Tuple[float, float], importance: str):
        """Data validator that tells if data is in a range. This applies to primitives (ints, floats).

        :param range: Inclusive range of parameters
        """
        super(DataInRangeValidatorPandas, self).__init__(importance=importance)
        self.range = range

    @classmethod
    def arg(cls) -> str:
        return 'range'

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, pd.Series)  # TODO -- handle dataframes?

    @classmethod
    def name(cls) -> str:
        return 'data_in_range_validator'

    def description(self) -> str:
        return f'Validates that the datapoint falls within the range ({self.range[0]}, {self.range[1]})'

    def validate(self, data: pd.Series) -> base.ValidationResult:
        min_, max_ = self.range
        between = data.between(min_, max_, inclusive=True)
        counts = between.value_counts().to_dict()
        in_range = counts.get(True, 0)
        out_range = counts.get(False, 0)
        passes = out_range == 0
        message = f'Series contains {in_range} values in range ({min_},{max_}), and {out_range} outside.'
        return base.ValidationResult(
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
    def __init__(self, range: Tuple[numbers.Real, numbers.Real], importance: str):
        """Data validator that tells if data is in a range. This applies to primitives (ints, floats).

        :param range: Inclusive range of parameters
        """
        super(DataInRangeValidatorPrimitives, self).__init__(importance=importance)
        self.range = range

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, numbers.Real)

    def description(self) -> str:
        return f'Validates that the datapoint falls within the range ({self.range[0]}, {self.range[1]})'

    def validate(self, data: numbers.Real) -> base.ValidationResult:
        min_, max_ = self.range
        passes = min_ <= data <= max_
        message = f'Data point {data} falls within acceptable range: ({min_}, {max_})' if passes else \
            f'Data point {data} does not fall within acceptable range: ({min_}, {max_})'
        return base.ValidationResult(
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

    @classmethod
    def name(cls) -> str:
        return 'data_in_range_validator'


class MaxFractionNansValidatorPandasSeries(BaseDefaultValidator):
    def __init__(self, max_fraction_nan: float, importance: str):
        super(MaxFractionNansValidatorPandasSeries, self).__init__(importance=importance)
        MaxFractionNansValidatorPandasSeries._validate_max_fraction_nan(max_fraction_nan)
        self.max_fraction_nan = max_fraction_nan

    @staticmethod
    def _to_percent(fraction: float):
        return '{0:.2%}'.format(fraction)

    @classmethod
    def name(cls) -> str:
        return 'max_fraction_nan_validator'

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, pd.Series)

    def description(self) -> str:
        return f'Validates that no more than {MaxFractionNansValidatorPandasSeries._to_percent(self.max_fraction_nan)} of the data is Nan.'

    def validate(self, data: pd.Series) -> base.ValidationResult:
        total_length = len(data)
        total_na = data.isna().sum()
        fraction_na = total_na / total_length
        passes = fraction_na <= self.max_fraction_nan
        return base.ValidationResult(
            passes=passes,
            message=f'Out of {total_length} items in the series, {total_na} of them are Nan, '
                    f'representing: {MaxFractionNansValidatorPandasSeries._to_percent(fraction_na)}. '
                    f'Max allowable Nans is: {MaxFractionNansValidatorPandasSeries._to_percent(self.max_fraction_nan)},'
                    f' so this {"passes" if passes else "does not pass"}.',
            diagnostics={
                'total_nan': total_na,
                'total_length': total_length,
                'fraction_na': fraction_na,
                'max_fraction_na': self.max_fraction_nan
            }
        )

    @classmethod
    def arg(cls) -> str:
        return 'max_fraction_nan'

    @staticmethod
    def _validate_max_fraction_nan(max_fraction_nan: float):
        if not (0 <= max_fraction_nan <= 1):
            raise ValueError(f'Maximum fraction allowed to be nan must be in range [0,1]')


class NansAllowedValidatorPandas(MaxFractionNansValidatorPandasSeries):
    def __init__(self, allow_nans: bool, importance: str):
        if allow_nans:
            raise ValueError(f'Only allowed to block Nans with this validator.'
                             f'Otherwise leave blank or specify the percentage of Nans using {MaxFractionNansValidatorPandasSeries.name()}')
        super(NansAllowedValidatorPandas, self).__init__(max_fraction_nan=0 if not allow_nans else 1.0, importance=importance)

    @classmethod
    def name(cls) -> str:
        return 'nans_allowed_validator'

    @classmethod
    def arg(cls) -> str:
        return 'allow_nans'


class DataTypeValidatorPandas(BaseDefaultValidator):

    def __init__(self, datatype: Type[Type], importance: str):
        super(DataTypeValidatorPandas, self).__init__(importance=importance)
        DataTypeValidatorPandas.datatype = datatype
        self.datatype = datatype

    @classmethod
    def name(cls) -> str:
        return 'dtype_validator'

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, pd.Series)

    def description(self) -> str:
        return f'Validates that the datatype of the pandas series is a subclass of: {self.datatype}'

    def validate(self, data: pd.Series) -> base.ValidationResult:
        dtype = data.dtype
        passes = np.issubdtype(dtype, self.datatype)
        return base.ValidationResult(
            passes=passes,
            message=f"Requires subclass of datatype: {self.datatype}. Got datatype: {dtype}. This {'is' if passes else 'is not'} a valid subclass.",
            diagnostics={
                'required_dtype': self.datatype,
                'actual_dtype': dtype
            }
        )

    @classmethod
    def arg(cls) -> str:
        return 'data_type'


class PandasMaxStandardDevValidator(BaseDefaultValidator):
    def __init__(self, max_standard_dev: float, importance: str):
        super(PandasMaxStandardDevValidator, self).__init__(importance)
        self.max_standard_dev = max_standard_dev

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, pd.Series)

    def description(self) -> str:
        return f'Validates that the standard deviation of a pandas series is no greater than : {self.max_standard_dev}'

    def validate(self, data: pd.Series) -> base.ValidationResult:
        standard_dev = data.std()
        passes = standard_dev <= self.max_standard_dev
        return base.ValidationResult(
            passes=passes,
            message=f'Max allowable standard dev is: {self.max_standard_dev}. '
                    f'Dataset stddev is : {standard_dev}. '
                    f"This {'passes' if passes else 'does not pass'}.",
            diagnostics={
                'standard_dev': standard_dev,
                'max_standard_dev': self.max_standard_dev
            }
        )

    @classmethod
    def arg(cls) -> str:
        return 'max_standard_dev'

    @classmethod
    def name(cls) -> str:
        return 'max_standard_dev_validator'


class PandasMeanInRangeValidator(BaseDefaultValidator):

    def __init__(self, mean_in_range: Tuple[float, float], importance: str):
        super(PandasMeanInRangeValidator, self).__init__(importance)
        self.mean_in_range = mean_in_range

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, pd.Series)

    def description(self) -> str:
        return f'Validates that a pandas series has mean in range [{self.mean_in_range[0]}, {self.mean_in_range[1]}]'

    def validate(self, data: pd.Series) -> base.ValidationResult:
        dataset_mean = data.mean()
        min_, max_ = self.mean_in_range
        passes = min_ <= dataset_mean <= max_
        return base.ValidationResult(
            passes=passes,
            message=f"Dataset has mean: {dataset_mean}. This {'is ' if passes else 'is not '} "
                    f'in the required range: [{self.mean_in_range[0]}, {self.mean_in_range[1]}].',
            diagnostics={
                'dataset_mean': dataset_mean,
                'mean_in_range': self.mean_in_range
            }
        )

    @classmethod
    def arg(cls) -> str:
        return 'mean_in_range'

    @classmethod
    def name(cls) -> str:
        return 'mean_in_range_validator'


AVAILABLE_DEFAULT_VALIDATORS = [
    DataInRangeValidatorPandas,
    DataInRangeValidatorPrimitives,
    PandasMaxStandardDevValidator,
    PandasMeanInRangeValidator,
    DataTypeValidatorPandas,
    MaxFractionNansValidatorPandasSeries,
    NansAllowedValidatorPandas,
]


def _append_pandera_to_default_validators():
    """Utility method to append pandera validators as needed"""
    try:
        import pandera
    except ModuleNotFoundError:
        logger.info(f'Cannot import pandera from pandera_validators. Run pip install hamilton[pandera] if needed.')
        return
    from hamilton.data_quality import pandera_validators
    AVAILABLE_DEFAULT_VALIDATORS.extend(pandera_validators.PANDERA_VALIDATORS)


_append_pandera_to_default_validators()


def resolve_default_validators(
        output_type: Type[Type],
        importance: str,
        available_validators: List[Type[BaseDefaultValidator]] = None,
        **default_validator_kwargs) -> List[BaseDefaultValidator]:
    """Resolves default validators given a set pof parameters and the type to which they apply.
    Note that each (kwarg, type) combination should map to a validator
    @param importance: importance level of the validator to instantiate
    @param output_type: The type to which the validator should apply
    @param available_validators: The available validators to choose from
    @param default_validator_kwargs: Kwargs to use
    @return: A list of validators to use
    """
    if available_validators is None:
        available_validators = AVAILABLE_DEFAULT_VALIDATORS
    validators = []
    for key in default_validator_kwargs.keys():
        for validator_cls in available_validators:
            if key == validator_cls.arg() and validator_cls.applies_to(output_type):
                validators.append(validator_cls(**{key: default_validator_kwargs[key], 'importance': importance}))
                break
        else:
            raise ValueError(f'No registered subclass of BaseDefaultValidator is available '
                             f'for arg: {key} and type {output_type}. This either means (a) this arg-type '
                             f"contribution isn't supported or (b) this has not been added yet (but should be). "
                             f'In the case of (b), we welcome contributions. Get started at github.com/stitchfix/hamilton')
    return validators
