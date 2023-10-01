import pandas as pd
import unittest
from io import StringIO
from pandas_extensions import PandasTableReader

class TestPandasTableReader(unittest.TestCase):

    def test_read_table(self):
        data = "Name\tAge\nAlice\t30\nBob\t40"
        buffer = StringIO(data)
        
        reader = PandasTableReader(buffer)
        df, _ = reader.load_data(pd.DataFrame)
        
        expected_df = pd.DataFrame({"Name": ["Alice", "Bob"], "Age": [30, 40]})
        pd.testing.assert_frame_equal(df, expected_df)

if __name__ == '__main__':
    unittest.main()
