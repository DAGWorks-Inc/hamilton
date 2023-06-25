import concurrent
import os.path
from typing import Dict, List, Tuple

import arxiv
import openai
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_random_exponential
from tqdm import tqdm


def arxiv_search_results(
    article_query: str,
    max_arxiv_results: int,
    sort_by: arxiv.SortCriterion.Relevance = arxiv.SortCriterion.Relevance,
) -> List[arxiv.Result]:
    _search = arxiv.Search(
        query=article_query,
        max_results=max_arxiv_results,
        sort_by=sort_by,
    )
    return list(_search.results())


def arxiv_result(arxiv_search_results: List[arxiv.Result]) -> List[Dict[str, str]]:
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

    return result_list


@retry(wait=wait_random_exponential(min=1, max=40), stop=stop_after_attempt(3))
def _get_embedding(text: str, model_name: str) -> Tuple[str, List[float]]:
    response = openai.Embedding.create(input=text, model=model_name)
    return text, response["data"][0]["embedding"]


def arxiv_result_embeddings(
    arxiv_search_results: List[arxiv.Result],
    embedding_model_name: str,
    max_num_concurrent_requests: int,
) -> pd.DataFrame:
    embedding_list = []
    index_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_num_concurrent_requests) as executor:
        futures = [
            executor.submit(
                _get_embedding,
                text=result.title,
                model_name=embedding_model_name,
            )
            for result in arxiv_search_results
        ]
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Generating embeddings",
        ):
            title, embedding = future.result()
            embedding_list.append(embedding)
            index_list.append(title)

    return pd.DataFrame({"embeddings": embedding_list}, index=index_list)


def arxiv_pdfs(
    arxiv_search_results: List[arxiv.Result], data_dir: str, max_num_concurrent_requests: int
) -> pd.DataFrame:
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

    return pd.DataFrame({"pdf_path": path_list}, index=index_list)


def arxiv_result_df(
    arxiv_pdfs: pd.DataFrame, arxiv_result_embeddings: pd.DataFrame
) -> pd.DataFrame:
    return pd.merge(arxiv_pdfs, arxiv_result_embeddings, left_index=True, right_index=True)


def save_arxiv_result_df(arxiv_result_df: pd.DataFrame, library_file_path: str) -> dict:
    if os.path.exists(library_file_path):
        arxiv_result_df.to_csv(library_file_path, mode="a", header=False)
    else:
        arxiv_result_df.to_csv(library_file_path)
    return {"num_articles_written": len(arxiv_result_df)}
