import logging

import functions

from hamilton import driver
from hamilton.plugins import h_diskcache


def main():
    dr = (
        driver.Builder()
        .with_modules(functions)
        .with_adapters(h_diskcache.DiskCacheAdapter())
        .build()
    )
    results = dr.execute(["C"], inputs=dict(external=10))
    print(results)
    results = dr.execute(["C"], inputs=dict(external=10))
    print(results)


if __name__ == "__main__":
    logger = logging.getLogger("hamilton.plugins.h_diskcache")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    main()
