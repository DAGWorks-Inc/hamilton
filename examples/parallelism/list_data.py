import dataclasses
import os
from typing import List

from hamilton.htypes import Parallelizable


def files(data_dir: str) -> List[str]:
    """Lists oll files in the data directory"""

    out = []
    for file in os.listdir(data_dir):
        if file.endswith(".csv"):
            out.append(os.path.join(data_dir, file))
    return out


@dataclasses.dataclass
class CityData:
    city: str
    weekend_file: str
    weekday_file: str


def city_data(files: List[str]) -> Parallelizable[CityData]:
    """Gathers a list of per-city data for processing/analyzing"""

    cities = dict()
    for file_name in files:
        city = os.path.basename(file_name).split("_")[0]
        is_weekend = file_name.endswith("weekends.csv")
        if city not in cities:
            cities[city] = CityData(city=city, weekend_file=None, weekday_file=None)
        if is_weekend:
            cities[city].weekend_file = file_name
        else:
            cities[city].weekday_file = file_name
    for city in cities.values():
        yield city
