import pandas as pd
import unittest
from pandas_extensions import CustomPandasLoader

class TestCustomPandasLoader(unittest.TestCase):
    def setUp(self):
        self.filepath = 'data.txt'

        # Create a sample data file
        with open(self.filepath, 'w') as f:
            f.write('A B C\n')
            f.write('1 2 3\n')
            f.write('4 5 6\n')

    def tearDown(self):
        import os
        os.remove(self.filepath)

    def test_load(self):
        loader = CustomPandasLoader(self.filepath, sep=' ')
        df = loader.load()

        expected_data = {'A': [1, 4], 'B': [2, 5], 'C': [3, 6]}
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(df, expected_df)

if __name__ == '__main__':
    unittest.main()
