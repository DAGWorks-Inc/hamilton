import os.path
from typing import Dict

import arxiv
import openai
import pandas as pd

from hamilton.function_modifiers import extract_fields
from hamilton.htypes import Collect, Parallel


def arxiv_search_result(
    article_query: str,
    max_arxiv_results: int,
    sort_by: arxiv.SortCriterion.Relevance = arxiv.SortCriterion.Relevance,
) -> Parallel[arxiv.Result]:
    """Goes to arxiv and returns a list articles that match the provided query.

    :param article_query: the query to search for.
    :param max_arxiv_results: the maximum number of results to return.
    :param sort_by: sort the results by this criterion.
    :return: list of arxiv.Result objects.
    """
    _search = arxiv.Search(
        query=article_query,
        max_results=max_arxiv_results,
        sort_by=sort_by,
    )
    for result in _search.results():
        yield result


@extract_fields({"title": str, "summary": str, "article_url": str, "pdf_url": str})
def result(arxiv_search_result: arxiv.Result) -> Dict[str, str]:
    return {
        "title": arxiv_search_result.title,
        "summary": arxiv_search_result.summary,
        "article_url": arxiv_search_result.links[0],
        "pdf_url": arxiv_search_result.links[1],
    }
    pass


def arxiv_result_embedding(
    title: str,
    embedding_model_name: str,
) -> list[float]:
    """Generates a pd.Series of embeddings, indexed by title for each arxiv search result.

    :param arxiv_search_results:
    :param embedding_model_name:
    :param max_num_concurrent_requests:
    :return: Series of embeddings, indexed by title.
    """
    response = openai.Embedding.create(input=title, model=embedding_model_name)
    return response["data"][0]["embedding"]


def arxiv_pdf(arxiv_search_result: arxiv.Result, data_dir: str) -> str:
    """Processes the arxiv search results and downloads the PDFs.

    :param arxiv_search_results: list of arxiv.Result objects.
    :param data_dir: the directory to save the PDFs to.
    :param max_num_concurrent_requests: the maximum number of concurrent requests.
    :return: a pd.Series of the filepaths to the PDFs, indexed by title.
    """
    return arxiv_search_result.download_pdf(dirpath=data_dir)


def arxiv_processed_result(
    title: str,
    summary: str,
    article_url: str,
    pdf_url: str,
    arxiv_pdfs: str,
    arxiv_result_embeddings: list[float],
):
    """creates dict with parameters as keys/values"""
    return {
        "title": title,
        "summary": summary,
        "article_url": article_url,
        "pdf_url": pdf_url,
        "arxiv_pdfs": arxiv_pdfs,
        "arxiv_result_embeddings": arxiv_result_embeddings,
    }


def arxiv_result_df(arxiv_processed_result: Collect[Dict[str, str]]):
    all_results = list(arxiv_processed_result)

    return pd.DataFrame.from_records(all_results).set_index("title", drop=False)


def save_arxiv_result_df(arxiv_result_df: pd.DataFrame, library_file_path: str) -> dict:
    """Saves the arxiv result dataframe to a csv file.

    Appends if it already exists. Does not protect against duplicates.

    :param arxiv_result_df: the dataframe to save.
    :param library_file_path: the path to the library file.
    :return: a dictionary with the number of articles newly written.
    """
    if os.path.exists(library_file_path):
        arxiv_result_df.to_csv(library_file_path, mode="a", header=False, index=False)
    else:
        arxiv_result_df.to_csv(library_file_path, index=False)
    return {"num_articles_written": len(arxiv_result_df)}
