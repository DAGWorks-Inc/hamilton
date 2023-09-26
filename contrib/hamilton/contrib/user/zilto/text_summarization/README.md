# Purpose of this module

This module implements a dataflow to summarize text hitting the OpenAI API.

You can pass in PDFs, or just text and it will get chunked and summarized by the OpenAI API.

# Configuration Options
This module can be configured with the following options:
 - {"file_type":  "pdf"} to read PDFs.
 - {"file_type":  "text"} to read a text file.
 - {} to have `raw_text` be passed in.

# Limitations

This module is limited to OpenAI.

It does not check the context length, so it may fail if the context is too long.
