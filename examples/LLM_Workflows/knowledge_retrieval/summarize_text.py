import ast
import concurrent
from typing import Callable, Generator, List

import openai
import pandas as pd
import tiktoken
from PyPDF2 import PdfReader
from scipy import spatial
from tenacity import retry, stop_after_attempt, wait_random_exponential
from tqdm import tqdm

from hamilton.function_modifiers import extract_columns


def summary_prompt() -> str:
    return "Summarize this text from an academic paper. Extract any key points with reasoning.\n\nContent:"


def main_summary_prompt() -> str:
    return """Write a summary collated from this collection of key points extracted from an academic paper.
    The summary should highlight the core argument, conclusions and evidence, and answer the user's query.
    User query: {query}
    The summary should be structured in bulleted lists following the headings Core Argument, Evidence, and Conclusions.
    Key points:\n{results}\nSummary:\n"""


@retry(wait=wait_random_exponential(min=1, max=40), stop=stop_after_attempt(3))
def user_query_embedding(user_query: str, embedding_model_name: str) -> List[float]:
    response = openai.Embedding.create(input=user_query, model=embedding_model_name)
    return response["data"][0]["embedding"]


def relatedness(
    user_query_embedding: List[float],
    embeddings: pd.Series,
    relatedness_fn: Callable = lambda x, y: 1 - spatial.distance.cosine(x, y),
) -> pd.Series:
    return embeddings.apply(lambda x: relatedness_fn(user_query_embedding, x))


def pdf_text(pdf_path: pd.Series) -> pd.Series:
    """Takes a filepath to a PDF and returns a string of the PDF's contents"""
    _pdf_text = []
    for i, file_path in pdf_path.items():
        # creating a pdf reader object
        reader = PdfReader(file_path)
        text = ""
        page_number = 0
        for page in reader.pages:
            page_number += 1
            text += page.extract_text() + f"\nPage Number: {page_number}"
        _pdf_text.append(text)
    return pd.Series(_pdf_text, index=pdf_path.index)


def _create_chunks(text: str, n: int, tokenizer: tiktoken.Encoding) -> Generator[str, None, None]:
    """Returns successive n-sized chunks from provided text.
    Split a text into smaller chunks of size n, preferably ending at the end of a sentence
    """
    tokens = tokenizer.encode(text)
    i = 0
    while i < len(tokens):
        # Find the nearest end of sentence within a range of 0.5 * n and 1.5 * n tokens
        j = min(i + int(1.5 * n), len(tokens))
        while j > i + int(0.5 * n):
            # Decode the tokens and check for full stop or newline
            chunk = tokenizer.decode(tokens[i:j])
            if chunk.endswith(".") or chunk.endswith("\n"):
                break
            j -= 1
        # If no end of sentence found, use n tokens as the chunk size
        if j == i + int(0.5 * n):
            j = min(i + n, len(tokens))
        yield tokens[i:j]
        i = j


def chunked_pdf_text(
    pdf_text: pd.Series, max_token_length: int, tokenizer_encoding: str = "cl100k_base"
) -> pd.Series:
    tokenizer = tiktoken.get_encoding(tokenizer_encoding)
    _chunked = pdf_text.apply(lambda x: _create_chunks(x, max_token_length, tokenizer))
    _chunked = _chunked.apply(lambda x: [tokenizer.decode(chunk) for chunk in x])
    return _chunked


def top_n_related_articles(
    relatedness: pd.Series, top_n: int, chunked_pdf_text: pd.Series
) -> pd.Series:
    """Returns the top_n related articles from the library_df"""
    return chunked_pdf_text[relatedness.sort_values(ascending=False).head(top_n).index]


@retry(wait=wait_random_exponential(min=1, max=40), stop=stop_after_attempt(3))
def _summarize_chunk(content: str, template_prompt: str, openai_gpt_model: str) -> str:
    """This function applies a prompt to some input content. In this case it returns a summarized chunk of text"""
    prompt = template_prompt + content
    response = openai.ChatCompletion.create(
        model=openai_gpt_model, messages=[{"role": "user", "content": prompt}], temperature=0
    )
    return response["choices"][0]["message"]["content"]


def summarized_pdf(
    top_n_related_articles: pd.Series, summary_prompt: str, openai_gpt_model: str
) -> str:
    """Only does first one..."""
    text_chunks = top_n_related_articles[0]
    results = ""
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(text_chunks)) as executor:
        futures = [
            executor.submit(_summarize_chunk, chunk, summary_prompt, openai_gpt_model)
            for chunk in text_chunks
        ]
        with tqdm(total=len(text_chunks)) as pbar:
            for _ in concurrent.futures.as_completed(futures):
                pbar.update(1)
        for future in futures:
            data = future.result()
            results += data
    return results


@extract_columns(*["pdf_path", "embeddings"])
def library_df(library_file_path: str) -> pd.DataFrame:
    _library_df = pd.read_csv(library_file_path)
    _library_df.columns = ["title", "pdf_path", "embeddings"]
    _library_df["embeddings"] = _library_df["embeddings"].apply(ast.literal_eval)
    _library_df.index = _library_df["title"]
    return _library_df


def summarize_text(
    user_query: str, summarized_pdf: str, main_summary_prompt: str, openai_gpt_model: str
) -> str:
    response = openai.ChatCompletion.create(
        model=openai_gpt_model,
        messages=[
            {
                "role": "user",
                "content": main_summary_prompt.format(query=user_query, results=summarized_pdf),
            }
        ],
        temperature=0,
    )
    return response["choices"][0]["message"]["content"]
