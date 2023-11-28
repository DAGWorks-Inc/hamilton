# --- START NOTICES (optional)
# --- END NOTICES
# --- START IMPORT SECTION
import logging

logger = logging.getLogger(__name__)

from hamilton import contrib

with contrib.catch_import_errors(__name__, __file__, logger):
    # non-hamilton imports go here
    pass

# hamilton imports go here; check for required version if need be.

# --- END IMPORT SECTION

# --- START HAMILTON DATAFLOW
import dataclasses
import logging
import os
from pathlib import Path
from typing import List

import boto3
import pandas as pd
from boto3 import Session

from hamilton.htypes import Collect, Parallelizable

# from hamilton.log_setup import setup_logging

logger = logging.getLogger(__name__)


def s3(aws_profile: str = "dagworks") -> boto3.resource:
    """Returns a boto3 resource for the 'aws_profile' profile"""

    # Create a session using the 'dagworks' profile
    session = Session(profile_name=aws_profile)

    # Use the session to create the S3 resource
    return session.resource("s3")


@dataclasses.dataclass
class ToDownload:
    key: str
    bucket: str


def ensured_save_dir(save_dir: str) -> str:
    if not os.path.exists(save_dir):
        Path(save_dir).mkdir()
    return save_dir


def downloadable(
    s3: boto3.resource, bucket: str, path_in_bucket: str, slice: int = None
) -> Parallelizable[ToDownload]:
    """Lists downloadables from the s3 bucket"""

    bucket_obj = s3.Bucket(bucket)
    objs = list(bucket_obj.objects.filter(Prefix=path_in_bucket).all())
    if slice is not None:
        objs = objs[:slice]
    logger.info(f"Found {len(objs)} objects in {bucket}/{path_in_bucket}")
    for obj in objs:
        yield ToDownload(key=obj.key, bucket=bucket)


def _already_downloaded(path: str) -> bool:
    """Checks if the data is already downloaded"""
    if os.path.exists(path):
        return True
    return False


def downloaded_data(
    downloadable: ToDownload,
    ensured_save_dir: str,
) -> str:
    """Downloads data, short-circuiting if the data already exists locally

    :param s3:
    :param bucket:
    :param path_in_bucket:
    :param save_dir:
    :return:
    """
    download_location = os.path.join(ensured_save_dir, downloadable.key)
    if _already_downloaded(download_location):
        logger.info(f"Already downloaded {download_location}")
        return download_location
    parent_path = os.path.dirname(download_location)
    if not os.path.exists(parent_path):
        os.makedirs(parent_path, exist_ok=True)
    s3_resource = s3()  # we want to ensure threadsafety --
    # we could do this in a pool, but for now we'll just create it cause we're doing this in
    # parallel

    bucket = s3_resource.Bucket(downloadable.bucket)
    bucket.download_file(downloadable.key, download_location)
    logger.info(f"Downloaded {download_location}")
    return download_location


def all_downloaded_data(downloaded_data: Collect[str]) -> List[str]:
    """Returns a list of all downloaded locations"""
    out = []
    for path in downloaded_data:
        out.append(path)
    return out


def _jsonl_parse(path: str) -> pd.DataFrame:
    """Loads a jsonl file into a dataframe"""
    df = pd.read_json(path, lines=True)
    return df[["created_at", "ip", "distinct_id", "timestamp", "person_id"]]


def processed_dataframe(all_downloaded_data: List[str]) -> pd.DataFrame:
    """Processes everything into a dataframe"""
    out = []
    for floc in all_downloaded_data:
        out.append(_jsonl_parse(floc))
    return pd.concat(out)


# --- END HAMILTON DATAFLOW
# --- START MAIN CODE
if __name__ == "__main__":
    # Code to create an imaging showing on DAG workflow.
    # run as a script to test Hamilton's execution
    import __init__ as MODULE_NAME
    from hamilton import base, driver

    dr = driver.Driver(
        {},  # CONFIG: fill as appropriate
        MODULE_NAME,
        adapter=base.DefaultAdapter(),
    )
    # saves to current working directory creating dag.png.
    dr.display_all_functions("dag", {"format": "png", "view": False})
# --- END MAIN CODE
