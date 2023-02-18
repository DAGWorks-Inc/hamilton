from typing import Any, Dict

import pandas as pd
import reusable_subdags

from hamilton.base import ResultMixin, SimplePythonGraphAdapter
from hamilton.driver import Driver

"""A pretty simple pipeline to demonstrate function reuse.
Note that this *also* demonstrates building a custom results builder!
This one does specific time-series methodology (upsampling...).
"""


class TimeSeriesJoinResultsBuilder(ResultMixin):
    def __init__(self, upsample_frequency: str):
        """Initializes a results builder that does a time-series join
        :param upsample_frequency: Argument to pandas sample() function.
        """
        self.sampling_methodology = upsample_frequency

    def resample(self, time_series: pd.Series):
        return time_series.resample(
            self.sampling_methodology
        ).bfill()  # TODO -- think through the right fill -- ffill()/bfill()/whatnot

    @staticmethod
    def is_time_series(series: Any):
        if not isinstance(series, pd.Series):
            return False
        if not series.index.inferred_type == "datetime64":
            return False
        return True

    def build_result(self, **outputs: Dict[str, Any]) -> Any:
        non_ts_output = [
            key
            for key, value in outputs.items()
            if not TimeSeriesJoinResultsBuilder.is_time_series(value)
        ]
        if len(non_ts_output) > 0:
            raise ValueError(
                f"All outputs from DAG must be time-series -- the following are not: {non_ts_output}"
            )
        resampled_results = {key: self.resample(value) for key, value in outputs.items()}
        return pd.DataFrame(resampled_results).bfill()


def main():
    dr = Driver(
        {},
        reusable_subdags,
        adapter=SimplePythonGraphAdapter(
            result_builder=TimeSeriesJoinResultsBuilder(upsample_frequency="D")
        ),
    )

    result = dr.execute(
        [
            "daily_unique_users_US",
            "daily_unique_users_CA",
            "weekly_unique_users_US",
            "weekly_unique_users_CA",
            "monthly_unique_users_US",
            "monthly_unique_users_CA",
        ]
    )

    print(result)


if __name__ == "__main__":
    main()
