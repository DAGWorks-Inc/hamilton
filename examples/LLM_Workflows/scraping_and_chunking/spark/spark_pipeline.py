"""
This module creates a spark job that
uses the sitemap.xml file to create a dataframe with a URL column,
that we then process using a Hamilton module to create a dataframe with
columns for the article text and the chunked text.
"""

import re

import doc_pipeline
import pyspark.sql as ps
import requests

from hamilton.plugins import h_spark


def spark_session(app_name: str) -> ps.SparkSession:
    return ps.SparkSession.builder.appName(app_name).getOrCreate()


def sitemap_text(sitemap_url: str = "https://hamilton.dagworks.io/en/latest/sitemap.xml") -> str:
    """Takes in a sitemap URL and returns the sitemap.xml file.

    :param sitemap_url: the URL of sitemap.xml file
    :return:
    """
    sitemap = requests.get(sitemap_url)
    return sitemap.text


def urls_from_sitemap(sitemap_text: str, spark_session: ps.SparkSession) -> ps.DataFrame:
    """Takes in a sitemap.xml file contents and creates a df of all the URLs in the file.

    :param sitemap_text: the contents of a sitemap.xml file
    :return: df of URLs
    """
    urls = re.findall(r"<loc>(.*?)</loc>", sitemap_text)
    df = spark_session.createDataFrame(urls, "string").toDF("url")
    return df


"""
with_columns makes some assumptions:
(a) that all functions in the with_columns subdag take in the dataframe
(b) that all intermediate functions in the subdag need to become columns in the dataframe
(c) it doesn't allow you to wire through inputs to the subdag
"""


@h_spark.with_columns(
    doc_pipeline,
    select=["article_text", "chunked_text"],
    columns_to_pass=["url"],
)
def chunked_url_text(urls_from_sitemap: ps.DataFrame) -> ps.DataFrame:
    """Creates dataframe with chunked text from URLs appended as columns.

    :param urls_from_sitemap:
    :return:
    """
    return urls_from_sitemap  # this has the new columns article_text and chunked_text


if __name__ == "__main__":
    from langchain import text_splitter

    # from langchain_core import documents
    # these two functions are here because we can't
    # use them with doc_pipeline due to with_column constraints.
    # so need to pass them in as inputs.
    def html_chunker() -> text_splitter.HTMLHeaderTextSplitter:
        """Return HTML chunker object.

        :return:
        """
        headers_to_split_on = [
            ("h1", "Header 1"),
            ("h2", "Header 2"),
            ("h3", "Header 3"),
        ]
        return text_splitter.HTMLHeaderTextSplitter(headers_to_split_on=headers_to_split_on)

    def text_chunker(
        chunk_size: int = 256, chunk_overlap: int = 32
    ) -> text_splitter.RecursiveCharacterTextSplitter:
        """Returns the text chunker object.

        :param chunk_size:
        :param chunk_overlap:
        :return:
        """
        return text_splitter.RecursiveCharacterTextSplitter(
            chunk_size=chunk_size, chunk_overlap=chunk_overlap
        )

    import spark_pipeline

    from hamilton import driver

    dr = driver.Builder().with_modules(spark_pipeline).with_config({}).build()
    dr.display_all_functions("pipeline.png")
    result = dr.execute(
        ["chunked_url_text"],
        inputs={
            "app_name": "chunking_spark_job",
            "html_chunker": html_chunker(),
            "text_chunker": text_chunker(),
        },
    )
    print(result["chunked_url_text"].show())
