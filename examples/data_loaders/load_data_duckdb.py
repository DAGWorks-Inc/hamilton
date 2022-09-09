import duckdb
import pandas as pd


def connection(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=db_path)


def spend(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute("select * from marketing_spend").fetchdf()


def churn(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute("select * from churn").fetchdf()


def signups(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute("select * from signups").fetchdf()
