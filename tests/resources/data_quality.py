import pandas as pd

from hamilton.function_modifiers import check_output


# @check_output(data_type=np.float) # TODO -- enable this once we fix the double-decorator issue
@check_output(range=(0, 1), importance="fail")
def data_might_be_in_range(data_quality_should_fail: bool) -> pd.Series:
    if data_quality_should_fail:
        return pd.Series([10.0])
    return pd.Series([0.5])


# TODO -- enable this once we fix the double-data-quality decorators with the same name bug
# @check_output(data_type=np.float)
# @check_output(range=(0, 1))
# def multi_layered_validator(data_quality_should_fail: bool) -> pd.Series:
#     if data_quality_should_fail:
#         return pd.Series([10.0])
#     return pd.Series([0.5])
