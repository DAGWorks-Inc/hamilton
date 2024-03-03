import json
import re

import requests
from langchain import text_splitter

# from langchain_core import documents


def article_regex() -> str:
    """This assumes you're using the furo theme for sphinx"""
    return r'<article role="main" id="furo-main-content">(.*?)</article>'


def article_text(url: str, article_regex: str) -> str:
    """Pulls URL and takes out relevant HTML.

    :param url: the url to pull.
    :param article_regex: the regext to use to get the contents out of.
    :return: sub-portion of the HTML
    """
    html = requests.get(url)
    article = re.findall(article_regex, html.text, re.DOTALL)
    if not article:
        raise ValueError(f"No article found in {url}")
    text = article[0].strip()
    return text


# def html_chunker() -> text_splitter.HTMLHeaderTextSplitter:
#     """Return HTML chunker object.
#
#     :return:
#     """
#     headers_to_split_on = [
#         ("h1", "Header 1"),
#         ("h2", "Header 2"),
#         ("h3", "Header 3"),
#     ]
#     return text_splitter.HTMLHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
#
#
# def text_chunker(
#     chunk_size: int = 256, chunk_overlap: int = 32
# ) -> text_splitter.RecursiveCharacterTextSplitter:
#     """Returns the text chunker object.
#
#     :param chunk_size:
#     :param chunk_overlap:
#     :return:
#     """
#     return text_splitter.RecursiveCharacterTextSplitter(
#         chunk_size=chunk_size, chunk_overlap=chunk_overlap
#     )


def chunked_text(
    article_text: str,
    html_chunker: text_splitter.HTMLHeaderTextSplitter,
    text_chunker: text_splitter.RecursiveCharacterTextSplitter,
) -> list[str]:
    """This function takes in HTML, chunks it, and then chunks it again.

    It then outputs a list of langchain "documents". Multiple documents for one HTML header section is possible.

    :param article_text:
    :param html_chunker:
    :param text_chunker:
    :return: need to return something we can make a pyspark column with
    """
    header_splits = html_chunker.split_text(article_text)
    splits = text_chunker.split_documents(header_splits)
    # TODO: make this a struct field compatible structure
    return [json.dumps(s.to_json()) for s in splits]
