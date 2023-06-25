import arxiv_articles
import summarize_text

from hamilton import base, driver


def populate_arxiv_library(query: str):
    """Populate the arxiv library with articles from the query."""
    dr = driver.Driver({}, arxiv_articles, adapter=base.SimplePythonGraphAdapter(base.DictResult()))
    inputs = {
        "embedding_model_name": "text-embedding-ada-002",
        "max_arxiv_results": 5,
        "article_query": query,
        "max_num_concurrent_requests": 5,
        "data_dir": "./data",
        "library_file_path": "./data/arxiv_library.csv",
    }
    dr.display_all_functions("./populate_arxiv_library", {"format": "png"})
    result = dr.execute(["arxiv_result_df", "save_arxiv_result_df"], inputs=inputs)
    print(result["save_arxiv_result_df"])
    print(result["arxiv_result_df"].head())


def answer_question(query: str):
    """Answer a question using the arxiv library."""
    dr = driver.Driver({}, summarize_text, adapter=base.SimplePythonGraphAdapter(base.DictResult()))
    inputs = {
        "embedding_model_name": "text-embedding-ada-002",
        "openai_gpt_model": "gpt-3.5-turbo-0613",
        "user_query": query,
        "top_n": 5,
        "max_token_length": 1500,
        "library_file_path": "./data/arxiv_library.csv",
    }
    dr.display_all_functions("./answer_question", {"format": "png"})
    result = dr.execute(["summarize_text"], inputs=inputs)
    print(result["summarize_text"])


if __name__ == "__main__":
    from hamilton import log_setup

    log_setup.setup_logging(log_level=log_setup.LOG_LEVELS["DEBUG"])
    populate_arxiv_library("ppo reinforcement learning")
    answer_question("PPO reinforcement learning sequence generation")
