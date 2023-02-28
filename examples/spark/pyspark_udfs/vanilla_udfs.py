"""Pyspark UDFs

Has to only contain map operations!
"""


def spend_per_signup(spend: float, signups: float) -> float:
    """The cost per signup in relation to spend."""
    return spend / signups


def augmented_mean(foo: float, bar: float) -> float:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return foo + bar


def spend_zero_mean(spend: float, spend_mean: float) -> float:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend - spend_mean


def spend_zero_mean_unit_variance(spend_zero_mean: float, spend_std_dev: float) -> float:
    """Function showing one way to make spend have zero mean and unit variance."""
    return spend_zero_mean / spend_std_dev
