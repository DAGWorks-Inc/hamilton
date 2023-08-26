import modin.pandas as pd


def base_table(tablename: str, db_connection: object) -> pd.DataFrame:
    # Select Data Source form configured DB
    df = pd.read_sql(tablename, con=db_connection)
    return df


def filtered_table(base_table: pd.DataFrame) -> pd.DataFrame:
    # keep columns that have more than 50% non-null values
    keep = base_table.columns[
        (((base_table.isnull().sum() / base_table.shape[0])) * 100 < 50).values
    ]
    return base_table[keep]


def cleaned_table(filtered_table: pd.DataFrame) -> pd.DataFrame:
    upper_bound = filtered_table.annual_inc.quantile(0.95)
    lower_bound = filtered_table.annual_inc.quantile(0.05)

    no_outliers = filtered_table[
        (filtered_table.annual_inc < upper_bound) & (filtered_table.annual_inc > lower_bound)
    ]

    print("{} outliers removed".format(filtered_table.shape[0] - no_outliers.shape[0]))

    return no_outliers


def final_table(cleaned_table: pd.DataFrame) -> pd.DataFrame:
    return pd.get_dummies(cleaned_table, columns="grade")
