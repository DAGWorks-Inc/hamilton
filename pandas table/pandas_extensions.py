import pandas as pd

class CustomPandasLoader:
    def __init__(self, filepath, **kwargs):
        self.filepath = filepath
        self.kwargs = kwargs

    def load(self):
        return pd.read_table(self.filepath, **self.kwargs)
