import logging
from typing import Any, List

logger = logging.getLogger(__name__)

from hamilton import contrib

with contrib.catch_import_errors(__name__, __file__, logger):
    from bs4 import BeautifulSoup
    import lxml  # noqa: F401
    import requests
    from tenacity import retry, stop_after_attempt, wait_random_exponential

import dataclasses

from hamilton.htypes import Collect, Parallelizable


@dataclasses.dataclass
class ParsingResult:
    """Result from the parsing function

    :param url: url to the HTML page
    :param parsed: the result of the parsing function
    """

    url: str
    parsed: Any


def url(urls: List[str]) -> Parallelizable[str]:
    """Iterate over the list of urls and create one branch per url

    :param urls: list of url to scrape and parse
    :return: a single url to scrape and parse
    """
    for url_ in urls:
        yield url_


@retry(wait=wait_random_exponential(min=1, max=40), stop=stop_after_attempt(3))
def html_page(url: str) -> str:
    """Get the HTML page as string
    The tenacity decorator sets the timeout and retry logic

    :param url: a single url to request
    :return: the HTML page as a string
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def parsed_html(
    url: str,
    html_page: str,
    tags_to_extract: List[str] = ["p", "li", "div"],
    tags_to_remove: List[str] = ["script", "style"],
) -> ParsingResult:
    """Parse an HTML string using BeautifulSoup

    :param url: the url of the requested page
    :param html_page: the HTML page associated with the url
    :param tags_to_extract: HTML tags to extract and gather
    :param tags_to_remove: HTML tags to remove
    :return: the ParsingResult which contains the url and the parsing results
    """
    soup = BeautifulSoup(html_page, features="lxml")

    for tag in tags_to_remove:
        for element in soup.find_all(tag):
            element.decompose()

    content = []
    for tag in tags_to_extract:
        for element in soup.find_all(tag):
            if tag == "a":
                href = element.get("href")
                if href:
                    content.append(f"{element.get_text()} ({href})")
                else:
                    content.append(element.get_text(strip=True))
            else:
                content.append(element.get_text(strip=True))
    content = " ".join(content)

    return ParsingResult(url=url, parsed=content)


def parsed_html_collection(parsed_html: Collect[ParsingResult]) -> List[ParsingResult]:
    """Collect parallel branches of `parsed_html`

    :param parsed_html: receive the ParsingResult associated with each url
    :return: list of ParsingResult
    """
    return list(parsed_html)
