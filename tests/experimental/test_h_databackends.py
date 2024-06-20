import pandas as pd

from hamilton.experimental import h_databackends


def test_isinstance_dataframe():
    value = pd.DataFrame()
    assert isinstance(value, h_databackends.DATAFRAME_TYPES)


def test_issubclass_dataframe():
    class_ = pd.DataFrame
    assert issubclass(class_, h_databackends.DATAFRAME_TYPES)


def test_not_isinstance_dataframe():
    value = 6
    assert not isinstance(value, h_databackends.DATAFRAME_TYPES)


def test_not_issubclass_dataframe():
    class_ = int
    assert not issubclass(class_, h_databackends.DATAFRAME_TYPES)


def test_isinstance_column():
    value = pd.Series()
    assert isinstance(value, h_databackends.COLUMN_TYPES)


def test_issubclass_column():
    class_ = pd.Series
    assert issubclass(class_, h_databackends.COLUMN_TYPES)


def test_not_isinstance_column():
    value = 6
    assert not isinstance(value, h_databackends.COLUMN_TYPES)


def test_not_issubclass_column():
    class_ = int
    assert not issubclass(class_, h_databackends.COLUMN_TYPES)
