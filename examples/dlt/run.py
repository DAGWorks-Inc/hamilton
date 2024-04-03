import os

import dlt
import slack
import transform

from hamilton import driver


def main(
    selected_channels: list[str],
    ingestion_full_refresh: bool = False,
):
    """ELT pipeline to load Slack messages and replies,
    then reassemble threads and use an LLM to summarize it.

    dlt does "Extract, Load"; Hamilton does "Transform"
    """

    # dlt
    source = slack.slack_source(
        selected_channels=selected_channels,
        replies=True,
    )

    slack_pipeline = dlt.pipeline(
        pipeline_name="slack",
        destination="duckdb",
        dataset_name="slack_data",
        full_refresh=ingestion_full_refresh,
    )

    load_info = slack_pipeline.run(source)

    if os.environ.get("OPENAI_API_KEY") is None:
        raise KeyError("OPENAI_API_KEY wasn't set.")

    # hamilton
    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(transform)
        .build()
    )
    dr.display_all_functions("dag_transform.png", orient="TB")

    inputs = dict(
        selected_channels=selected_channels,
        dlt_load_info=load_info.asdict(),
    )
    dr.execute(["threads"], inputs=inputs)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("channels", nargs="+", type=str, help="Slack channels to load.")
    parser.add_argument("--full-refresh", action="store_true", help="Reload all Slack data.")

    args = parser.parse_args()

    main(selected_channels=args.channels, ingestion_full_refresh=args.full_refresh)
