import pandas as pd

from hamilton import driver
from hamilton.plugins.h_slack import SlackNotifier


def test_function() -> pd.DataFrame:
    raise Exception("test exception")
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


if __name__ == "__main__":
    import __main__

    api_key = "YOUR_API_KEY"
    channel = "YOUR_CHANNEL"
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_adapters(SlackNotifier(api_key=api_key, channel=channel))
        .build()
    )
    print(dr.execute(["test_function"]))
