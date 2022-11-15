import numpy as np


class CustomType:
    def __init__(self, value: int):
        self.value = value


def add_custom_type(a: CustomType, b: CustomType) -> int:
    """adds two custom types, these should be class methods but this is just an example"""
    return a.value + b.value
