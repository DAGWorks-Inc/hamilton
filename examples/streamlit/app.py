from typing import Optional

import logic
import streamlit as st

from hamilton import driver


# cache to avoid rebuilding the Driver
@st.cache_resource
def get_hamilton_driver() -> driver.Driver:
    return driver.Builder().with_modules(logic).build()


# cache results for the set of inputs
@st.cache_data
def _execute(
    final_vars: list[str],
    inputs: Optional[dict] = None,
    overrides: Optional[dict] = None,
) -> dict:
    """Generic utility to cache Hamilton results"""
    dr = get_hamilton_driver()
    return dr.execute(final_vars, inputs=inputs, overrides=overrides)


def get_state_inputs() -> dict:
    keys = ["selected_job"]
    return {k: v for k, v in st.session_state.items() if k in keys}


def get_state_overrides() -> dict:
    keys = []
    return {k: v for k, v in st.session_state.items() if k in keys}


def execute(final_vars: list[str]):
    return _execute(final_vars, get_state_inputs(), get_state_overrides())


def app():
    st.title("Hamilton + Streamlit 🐱‍🚀")

    # run the base data that always needs to be displayed
    data = execute(["all_jobs", "balance_per_job", "balance_per_job_boxplot"])

    # display the base dataframe and plotly chart
    st.dataframe(data["balance_per_job"])
    st.plotly_chart(data["balance_per_job_boxplot"])

    # get the selection options from `data`
    # store the selection in the state `selected_job`
    st.selectbox("Select a job", options=data["all_jobs"], key="selected_job")
    # get the value from the dict
    st.plotly_chart(execute(["job_hist"])["job_hist"])


if __name__ == "__main__":
    app()
