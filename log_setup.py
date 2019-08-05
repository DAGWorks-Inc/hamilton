import logging
import sys


def setup_logging():
    """Helper function to setup logging to console."""
    root_logger = logging.getLogger('')  # root logger
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(name)s(%(lineno)s): %(message)s')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)
    root_logger.setLevel(logging.INFO)
