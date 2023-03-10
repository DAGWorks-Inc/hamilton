import numpy as np
import pandas as pd


def spend() -> pd.DataFrame:
    data = np.array(
        [
            (
                "2022-08-03T00:00:00.000000000",
                104052.98074001,
                115300.21226012,
                69384.46649019,
                49474.45580366,
                12851.6540992,
                1498.5114764,
                "2022-08-03T00:00:00.000000000",
            ),
            (
                "2022-08-04T00:00:00.000000000",
                103234.15793884,
                115326.0151612,
                71113.31018247,
                52513.19734904,
                12344.42778548,
                1033.79398268,
                "2022-08-04T00:00:00.000000000",
            ),
            (
                "2022-08-05T00:00:00.000000000",
                101816.40188563,
                115194.04661767,
                71367.20874633,
                51795.51413309,
                11536.41253561,
                2101.46146166,
                "2022-08-05T00:00:00.000000000",
            ),
            (
                "2022-08-06T00:00:00.000000000",
                102263.53043232,
                115601.2888751,
                71474.76280964,
                52861.22158421,
                11652.28867968,
                1046.83170946,
                "2022-08-06T00:00:00.000000000",
            ),
            (
                "2022-08-07T00:00:00.000000000",
                103271.09660695,
                115306.96341012,
                71888.99025677,
                50742.70043588,
                11160.23631976,
                2521.31311947,
                "2022-08-07T00:00:00.000000000",
            ),
            (
                "2022-08-08T00:00:00.000000000",
                100775.86701231,
                116634.88666304,
                71603.50462531,
                52361.08798097,
                12869.33161266,
                3269.57027156,
                "2022-08-08T00:00:00.000000000",
            ),
            (
                "2022-08-09T00:00:00.000000000",
                101527.74726883,
                114868.8422755,
                70260.81680881,
                49647.9754876,
                13187.07115589,
                2134.71274923,
                "2022-08-09T00:00:00.000000000",
            ),
            (
                "2022-08-10T00:00:00.000000000",
                101150.73295175,
                114941.32547639,
                68802.02668922,
                49590.55466274,
                13129.31334755,
                3328.0293293,
                "2022-08-10T00:00:00.000000000",
            ),
            (
                "2022-08-11T00:00:00.000000000",
                100317.64365959,
                115682.20050942,
                67735.95105252,
                50621.23723767,
                14019.11780391,
                2360.4382216,
                "2022-08-11T00:00:00.000000000",
            ),
            (
                "2022-08-12T00:00:00.000000000",
                102024.067597,
                116770.81592363,
                66244.22984364,
                49503.73825509,
                14533.2726457,
                1868.18205207,
                "2022-08-12T00:00:00.000000000",
            ),
        ],
        dtype=[
            ("index", "<M8[ns]"),
            ("facebook", "<f8"),
            ("twitter", "<f8"),
            ("tv", "<f8"),
            ("youtube", "<f8"),
            ("radio", "<f8"),
            ("billboards", "<f8"),
            ("date", "<M8[ns]"),
        ],
    )
    return pd.DataFrame.from_records(data)


def churn() -> pd.DataFrame:
    data = np.array(
        [
            ("2022-08-03T00:00:00.000000000", 160, 53, "2022-08-03T00:00:00.000000000"),
            ("2022-08-04T00:00:00.000000000", 162, 54, "2022-08-04T00:00:00.000000000"),
            ("2022-08-05T00:00:00.000000000", 162, 50, "2022-08-05T00:00:00.000000000"),
            ("2022-08-06T00:00:00.000000000", 161, 53, "2022-08-06T00:00:00.000000000"),
            ("2022-08-07T00:00:00.000000000", 160, 49, "2022-08-07T00:00:00.000000000"),
            ("2022-08-08T00:00:00.000000000", 160, 52, "2022-08-08T00:00:00.000000000"),
            ("2022-08-09T00:00:00.000000000", 161, 53, "2022-08-09T00:00:00.000000000"),
            ("2022-08-10T00:00:00.000000000", 160, 57, "2022-08-10T00:00:00.000000000"),
            ("2022-08-11T00:00:00.000000000", 156, 56, "2022-08-11T00:00:00.000000000"),
            ("2022-08-12T00:00:00.000000000", 148, 58, "2022-08-12T00:00:00.000000000"),
        ],
        dtype=[("index", "<M8[ns]"), ("womens", "<i8"), ("mens", "<i8"), ("date", "<M8[ns]")],
    )
    return pd.DataFrame.from_records(data)


def signups() -> pd.DataFrame:
    data = np.array(
        [
            ("2022-08-03T00:00:00.000000000", 2184, 429, "2022-08-03T00:00:00.000000000"),
            ("2022-08-04T00:00:00.000000000", 2164, 461, "2022-08-04T00:00:00.000000000"),
            ("2022-08-05T00:00:00.000000000", 2159, 454, "2022-08-05T00:00:00.000000000"),
            ("2022-08-06T00:00:00.000000000", 2157, 449, "2022-08-06T00:00:00.000000000"),
            ("2022-08-07T00:00:00.000000000", 2121, 478, "2022-08-07T00:00:00.000000000"),
            ("2022-08-08T00:00:00.000000000", 2151, 517, "2022-08-08T00:00:00.000000000"),
            ("2022-08-09T00:00:00.000000000", 2133, 541, "2022-08-09T00:00:00.000000000"),
            ("2022-08-10T00:00:00.000000000", 2160, 565, "2022-08-10T00:00:00.000000000"),
            ("2022-08-11T00:00:00.000000000", 2135, 609, "2022-08-11T00:00:00.000000000"),
            ("2022-08-12T00:00:00.000000000", 2116, 633, "2022-08-12T00:00:00.000000000"),
        ],
        dtype=[("index", "<M8[ns]"), ("womens", "<i8"), ("mens", "<i8"), ("date", "<M8[ns]")],
    )
    return pd.DataFrame.from_records(data)
