# import os

from hamilton_sdk import adapters

from hamilton import driver

# from hamilton.plugins import h_ray
from hamilton.execution import executors


def run_parallel():
    tracker = adapters.HamiltonTracker(
        project_id=7,  # modify this as needed
        username="user@example.com",  # modify this as needed
        dag_name="my_version_of_the_dag",
        tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"},
    )
    import ner_extraction_parallel

    dr = (
        driver.Builder()
        .with_config({})
        .with_modules(ner_extraction_parallel)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executors.MultiThreadingExecutor(5))
        .with_adapters(tracker)  # h_tqdm.ProgressBar(), # lifecycle.PrintLn()
        .build()
    )
    dr.display_all_functions("ner_extraction_parallel.png")

    sampled_articles = dr.execute(["sampled_articles"])["sampled_articles"]
    # print(sampled_articles.iloc[0:64].copy())
    #
    retriever = dr.execute(["retriever"])["retriever"]
    #
    ner_pipeline = dr.execute(["ner_pipeline"])["ner_pipeline"]
    results = dr.execute(
        ["total_upserted", "lancedb_table"],
        inputs={"table_name": "temp1"},
        overrides={
            "retriever": retriever,
            "ner_pipeline": ner_pipeline,
            "sampled_articles": sampled_articles,
        },
    )
    cached = results.update(
        {"retriever": retriever, "ner_pipeline": ner_pipeline, "sampled_articles": sampled_articles}
    )
    print(results)
    query = "How Data is changing the world?"
    r = dr.execute(
        ["search_lancedb"], inputs={"query": query, "table_name": "temp1"}, overrides=cached
    )
    print(r)

    query = "Why does SpaceX want to build a city on Mars?"
    r = dr.execute(
        ["search_lancedb"], inputs={"query": query, "table_name": "temp1"}, overrides=cached
    )
    print(r)


def run_sequential():
    tracker = adapters.HamiltonTracker(
        project_id=7,  # modify this as needed
        username="elijah@dagworks.io",  # modify this as needed
        dag_name="my_version_of_the_dag",
        tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"},
    )
    import ner_extraction

    dr = (
        driver.Builder()
        .with_config({})
        .with_modules(ner_extraction)
        .with_adapters(tracker)  # h_tqdm.ProgressBar(), # lifecycle.PrintLn()
        .build()
    )
    dr.display_all_functions("ner_extraction.png")

    # results = dr.execute(
    #     ["load_into_lance_db"],
    #     inputs={"table_name": "temp2"},
    # )
    # print(results)
    query = "How Data is changing the world?"
    r = dr.execute(["search_lancedb"], inputs={"query": query, "table_name": "temp2"})
    print(r)

    query = "Why does SpaceX want to build a city on Mars?"
    r = dr.execute(["search_lancedb"], inputs={"query": query, "table_name": "temp2"})
    print(r)


if __name__ == "__main__":
    run_sequential()
    # run_parallel()
