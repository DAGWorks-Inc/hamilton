import pandas as pd
import plotly.express as px
from plotly.graph_objs import Figure


def base_df() -> pd.DataFrame:
    path = "https://raw.githubusercontent.com/Lexie88rus/bank-marketing-analysis/master/bank.csv"
    return pd.read_csv(path)


def all_jobs(base_df: pd.DataFrame) -> list[str]:
    return base_df["job"].unique().tolist()


def balance_per_job(base_df: pd.DataFrame) -> pd.DataFrame:
    return base_df.groupby("job")["balance"].describe().astype(int)


def balance_per_job_boxplot(base_df: pd.DataFrame) -> Figure:
    return px.box(base_df, x="job", y="balance")


def job_df(base_df: pd.DataFrame, selected_job: str) -> pd.DataFrame:
    return base_df.loc[base_df["job"] == selected_job]


def job_hist(job_df: pd.DataFrame) -> Figure:
    return px.histogram(job_df["balance"])
