from kedro.io import DataCatalog
from kedro.io.memory_dataset import MemoryDataset

from hamilton.plugins import kedro_extensions


def test_kedro_saver():
    dataset_name = "in_memory"
    data = 37
    catalog = DataCatalog({dataset_name: MemoryDataset()})

    saver = kedro_extensions.KedroSaver(dataset_name=dataset_name, catalog=catalog)
    saver.save_data(data)
    loaded_data = catalog.load(dataset_name)

    assert loaded_data == data


def test_kedro_loader():
    dataset_name = "in_memory"
    data = 37
    catalog = DataCatalog({dataset_name: MemoryDataset(data=data)})

    loader = kedro_extensions.KedroLoader(dataset_name=dataset_name, catalog=catalog)
    loaded_data, metadata = loader.load_data(int)

    assert loaded_data == data
