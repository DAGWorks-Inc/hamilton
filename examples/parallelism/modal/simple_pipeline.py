import pandas as pd
from h_modal import remote


def data() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@remote(source="modal")
def remote_processed_data(data: pd.DataFrame) -> pd.DataFrame:
    import time

    time.sleep(1)
    return data**2


def locally_gathered_data(remote_processed_data: pd.DataFrame) -> pd.DataFrame:
    print(remote_processed_data)
    return remote_processed_data
