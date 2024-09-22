from sklearn.datasets import fetch_species_distributions
from sklearn.utils._bunch import Bunch


def data() -> Bunch:
    return fetch_species_distributions()
