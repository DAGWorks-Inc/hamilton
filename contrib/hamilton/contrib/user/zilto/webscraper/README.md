# Purpose of this module

This module implements a simple webscraper that collects the specified HTML tags and removes undesirable ones. Simply give it a list of URLs.

Timeout and retry logic for HTTP request is implemented using the `tenacity` package

# Configuration Options
## Config.when
This module doesn't receive any configuration.

## Inputs
 - `urls` (Required): a list of valid url to scrape
 - `tags_to_extract`: a list of HTML tags to extract
 - `tags_to_remove`: a list of HTML tags to remove

## Overrides
 - `parsed_html`: if the function doesn't provide enough flexibility, another parser can be provided as long as it has parameters `url` and `html_page` and outputs a `ParsingResult` object.

# Limitations
- The timeout and retry values need to be edited via the decorator of `html_page()`.
