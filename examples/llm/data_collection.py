import feedparser
import unstructured
import weaviate
from unstructured.partition.html import partition_html
from unstructured.staging.weaviate import create_unstructured_weaviate_class, stage_for_weaviate

from hamilton.function_modifiers import config


def rss_feed_urls(rss_url_file_path: str) -> list[str]:
    """Read urls from a .txt file; each line is a url"""
    with open(rss_url_file_path, "r") as f:
        return f.readlines()


def rss_entry_urls(rss_feed_urls: list[str]) -> list[str]:
    """Iterate over RSS feeds to get the urls associated with each entry"""
    entry_urls = []
    for url in rss_feed_urls:
        rss_feed = feedparser.parse(url)
        urls = [e["link"] for e in rss_feed.entries if e.get("link")]
        entry_urls.extend(urls)

    return entry_urls


def rss_entry_url(rss_entry_urls: list[str]) -> str:
    return rss_entry_urls[0]


def html_chunks(rss_entry_url: str) -> list[unstructured.documents]:
    """create a list of text chunks from the HTML retrieved at the given url"""
    elements = partition_html(url=rss_entry_url, html_assemble_articles=True)
    return elements


@config.when_not(overwrite_schema=True)
def create_weaviate_schema__no_overwrite(
    weaviate_client: weaviate.Client, class_name: str = "ArticleDocument"
) -> None:
    """create a Weaviate schema and push it to Weaviate; do not overwrite if any existing schema"""
    if weaviate_client.schema.get()["classes"]:
        return

    unstructured_class = create_unstructured_weaviate_class(class_name=class_name)
    schema = {"classes": [unstructured_class]}
    weaviate_client.schema.create(schema)


@config.when(overwrite_schema=True)
def create_weaviate_schema__overwrite(
    weaviate_client: weaviate.Client, class_name: str = "ArticleDocument"
) -> None:
    """create a Weaviate schema and push it to Weaviate; overwrite existing schema"""
    unstructured_class = create_unstructured_weaviate_class(class_name=class_name)
    schema = {"classes": [unstructured_class]}
    weaviate_client.schema.create(schema)


def push_to_weaviate(
    weaviate_client: weaviate.Client,
    html_chunks: list[unstructured.documents],
    class_name: str = "ArticleDocument",
) -> None:
    """Push text chunks to Weaviate using a dynamic batching strategy"""
    data_objects = stage_for_weaviate(html_chunks)

    with weaviate_client.batch(batch_size=10, dynamic=True) as batch:
        for data_object in data_objects:
            batch.add_data_object(
                data_object,
                class_name,
                uuid=weaviate.util.generate_uuid5(data_object),
            )
