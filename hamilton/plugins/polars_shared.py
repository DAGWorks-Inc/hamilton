from typing import Dict, Any, Tuple, Type, Union

import polars as pl
from hamilton.io import utils
from hamilton.io.utils import get_file_metadata


class PolarsReaderWriter():

    file: str = ""

    def save_data(self, data: Union[pl.DataFrame, pl.LazyFrame], fmt: str, file: str, kwargs: Dict) -> Dict[str, Any]:
        self.file = file
        if fmt == "csv":
            return self.__save_csv_data(data, kwargs)

    def load_data(self, fmt: str, file: str, output: str, kwargs: Dict) -> Tuple[Union[pl.DataFrame, pl.LazyFrame], Dict[str, Any]]:
        self.file = file

        if fmt == "csv":
            return self.__load_csv_data(output, kwargs)

    def __save_csv_data(self, data: Union[pl.DataFrame, pl.LazyFrame], kwargs: Dict) -> Dict[str, Any]:
        if isinstance(data, pl.LazyFrame):
            data = data.collect()

        data.write_csv(self.file, **kwargs)
        return utils.get_file_and_dataframe_metadata(self.file, data)

    def __load_csv_data(self, output: str, kwargs: Dict) -> Tuple[Union[pl.DataFrame, pl.LazyFrame], Dict[str, Any]]:
        if output == "lazy":
            df = pl.scan_csv(self.file, **kwargs)
        else:
            df = pl.read_csv(self.file, **kwargs)


        metadata = utils.get_file_and_dataframe_metadata(self.file, df)
        return df, metadata
