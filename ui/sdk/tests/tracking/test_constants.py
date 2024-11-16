import configparser
from hamilton_sdk.tracking import constants


def test__convert_to_type():
    # using configparser to make it more realistic
    config = configparser.ConfigParser()
    config["SDK_CONSTANTS"] = {
        "CAPTURE_DATA_STATISTICS": "true",
        "MAX_LIST_LENGTH_CAPTURE": "5",
        "MAX_DICT_LENGTH_CAPTURE": "10",
        "SOMETHING_ELSE": "11.0",
        "Another_thing": "1asdfasdf",
    }
    assert constants._convert_to_type(config["SDK_CONSTANTS"]["CAPTURE_DATA_STATISTICS"]) is True
    assert constants._convert_to_type(config["SDK_CONSTANTS"]["MAX_LIST_LENGTH_CAPTURE"]) == 5
    assert constants._convert_to_type(config["SDK_CONSTANTS"]["MAX_DICT_LENGTH_CAPTURE"]) == 10
    assert constants._convert_to_type(config["SDK_CONSTANTS"]["SOMETHING_ELSE"]) == 11.0
    assert constants._convert_to_type(config["SDK_CONSTANTS"]["Another_thing"]) == "1asdfasdf"
    o = object()
    assert constants._convert_to_type(o) == o
