import pandas as pd
from list_data import CityData

from hamilton.function_modifiers import extract_columns, load_from, source


def weekend_file(city_data: CityData) -> str:
    """Returns the weekend file for a given city"""

    return city_data.weekend_file


def weekday_file(city_data: CityData) -> str:
    """Returns the weekday file for a given city"""

    return city_data.weekday_file


@load_from.csv(path=source("weekday_file"))
def weekday_data(wkd_data: pd.DataFrame) -> pd.DataFrame:
    return wkd_data


@load_from.csv(path=source("weekend_file"))
def weekend_data(wknd_data: pd.DataFrame) -> pd.DataFrame:
    return wknd_data


@extract_columns(
    "realSum", "room_type", "person_capacity", "guest_satisfaction_overall", "cleanliness_rating"
)
def all_data(weekday_data: pd.DataFrame, weekend_data: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([weekday_data, weekend_data])


def mean_guest_satisfaction(guest_satisfaction_overall: pd.Series) -> float:
    return guest_satisfaction_overall.mean()


def mean_price(realSum: pd.Series) -> float:
    return realSum.mean()


def mean_price_per_capacity(realSum: pd.Series, person_capacity: pd.Series) -> float:
    return (realSum / person_capacity).mean()


def mean_cleanliness(cleanliness_rating: pd.Series) -> float:
    return cleanliness_rating.mean()


def max_price(realSum: pd.Series) -> float:
    return realSum.max()


def statistics(
    mean_guest_satisfaction: float,
    mean_price: float,
    mean_price_per_capacity: float,
    mean_cleanliness: float,
    max_price: float,
    city_data: CityData,
) -> dict:
    """Basic aggregation to join the data together."""
    return {
        "mean_guest_satisfaction": mean_guest_satisfaction,
        "mean_price": mean_price,
        "mean_price_per_person": mean_price_per_capacity,
        "cleanliness_ratings_mean": mean_cleanliness,
        "max_price": max_price,
        "city": city_data.city,
    }
