import concurrent
import os.path
from typing import List, Tuple

import arxiv
import openai
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_random_exponential
from tqdm import tqdm

from hamilton.function_modifiers import extract_columns


def arxiv_search_results(
    article_query: str,
    max_arxiv_results: int,
    sort_by: arxiv.SortCriterion.Relevance = arxiv.SortCriterion.Relevance,
) -> List[arxiv.Result]:
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
    return list(_search.results())


@extract_columns(*["title", "summary", "article_url", "pdf_url"])
def arxiv_result(arxiv_search_results: List[arxiv.Result]) -> pd.DataFrame:
    """Processes arxiv search results into a list of dictionaries for easier processing.

    :param arxiv_search_results: list of arxiv.Result objects.
    :return: Dataframe of title, summary, article_url, pdf_url.
    """
    result_list = []
    for result in arxiv_search_results:
        _links = [x.href for x in result.links]
        result_list.append(
            {
                "title": result.title,
                "summary": result.summary,
                "article_url": _links[0],
                "pdf_url": _links[1],
            }
        )

    _df = pd.DataFrame(result_list)
    _df.index = _df["title"]
    return _df


@retry(wait=wait_random_exponential(min=1, max=40), stop=stop_after_attempt(3))
def _get_embedding(text: str, model_name: str) -> Tuple[str, List[float]]:
    """Helper function to get embeddings from OpenAI API.

    :param text: the text to embed.
    :param model_name: the name of the embedding model to use.
    :return: tuple of text and its embedding.
    """
    response = openai.Embedding.create(input=text, model=model_name)
    return text, response["data"][0]["embedding"]


def arxiv_result_embeddings(
    title: pd.Series,
    embedding_model_name: str,
    max_num_concurrent_requests: int,
) -> pd.Series:
    """Generates a pd.Series of embeddings, indexed by title for each arxiv search result.

    :param arxiv_search_results:
    :param embedding_model_name:
    :param max_num_concurrent_requests:
    :return: Series of embeddings, indexed by title.
    """
    embedding_list = []
    index_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_num_concurrent_requests) as executor:
        futures = [
            executor.submit(
                _get_embedding,
                text=_title,
                model_name=embedding_model_name,
            )
            for _, _title in title.items()
        ]
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Generating embeddings",
        ):
            title, embedding = future.result()
            embedding_list.append(embedding)
            index_list.append(title)

    return pd.Series(embedding_list, index=index_list)


def arxiv_pdfs(
    arxiv_search_results: List[arxiv.Result], data_dir: str, max_num_concurrent_requests: int
) -> pd.Series:
    """Processes the arxiv search results and downloads the PDFs.

    :param arxiv_search_results: list of arxiv.Result objects.
    :param data_dir: the directory to save the PDFs to.
    :param max_num_concurrent_requests: the maximum number of concurrent requests.
    :return: a pd.Series of the filepaths to the PDFs, indexed by title.
    """
    path_list = []
    index_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_num_concurrent_requests) as executor:
        futures = {
            executor.submit(
                result.download_pdf,
                dirpath=data_dir,
            ): result.title
            for result in arxiv_search_results
        }
        for future in tqdm(
            concurrent.futures.as_completed(futures.keys()),
            total=len(futures),
            desc="Saving PDFs",
        ):
            filepath = future.result()
            path_list.append(filepath)
            index_list.append(futures[future])

    return pd.Series(path_list, index=index_list)


def arxiv_result_df(
    title: pd.Series,
    summary: pd.Series,
    article_url: pd.Series,
    pdf_url: pd.Series,
    arxiv_pdfs: pd.Series,
    arxiv_result_embeddings: pd.Series,
) -> pd.DataFrame:
    """Creates dataframe representing the arxiv search results.

    :param title:
    :param summary:
    :param article_url:
    :param pdf_url:
    :param arxiv_pdfs: the location of the PDFs
    :param arxiv_result_embeddings:  the embeddings of the titles
    :return: a dataframe indexed by title with columns for pdf_path and embeddings
    """
    _df = pd.DataFrame(
        {
            "title": title,
            "pdf_path": arxiv_pdfs,
            "embeddings": arxiv_result_embeddings,
            "summary": summary,
            "article_url": article_url,
            "pdf_url": pdf_url,
        }
    )
    _df.index = _df["title"]
    return _df


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
