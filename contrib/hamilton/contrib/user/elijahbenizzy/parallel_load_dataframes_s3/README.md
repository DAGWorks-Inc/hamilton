# Purpose of this module
This module loads up dataframes specified in a `json` or `jsonl` set of files from s3.

You have to pass it:
1. The bucket to download from (`bucket`)
2. The path within the bucket to crawl (`path_in_bucket`)
3. The caching directory for saving files locally (`save_dir`)

Optionally, you can pass it:
1. The AWS profile (`aws_profile`), this defaults to `default`

This will look for all files that end with `json` or `jsonl`, `under s3://<bucket>/<path_within_bucket>` download them to the save_dir, load them, and concatenate it into a dataframe.
Note that if a file exists locally it is skipped -- thus this is idempotent.
# Configuration Options

N/A

# Limitations

This only downloads `json`/`jsonl` files, and currently crawls the entire sub-bucket. This also requires
that all files be able to be held in memory, and are of a uniform schema.
