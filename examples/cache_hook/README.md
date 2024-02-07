# Cache hook
This hook uses the [diskcache](https://grantjenks.com/docs/diskcache/tutorial.html) to cache node execution on disk. The cache key is a tuple of the function's `(source code, input a, ..., input n)`.

> 💡 This can be a great tool for developing inside a Jupyter notebook or other interactive environments.

Disk cache has great features to:
- set maximum cache size
- set automated eviction policy once maximum size is reached
- allow custom `Disk` implementations to change the serialization protocol (e.g., pickle, JSON)

> ⚠ The default `Disk` serializes objects using the `pickle` module. Changing Python or library versions could break your cache (both keys and values). Learn more about [caveats](https://grantjenks.com/docs/diskcache/tutorial.html#caveats).

> ❓ To store artifacts robustly, please use Hamilton materializers instead.


# How to use it
## Use the hook
Find it under plugins at `hamilton.plugins.h_diskcache` and add it to your Driver definition.

```python
from hamilton import driver
from hamilton.plugins import h_diskcache
import functions

dr = (
    driver.Builder()
    .with_modules(functions)
    .with_adapters(h_diskcache.CacheHook())
    .build()
)
```

## Inspect the hook
To inspect the caching behavior in real-time, you can get the logger:

```python
logger = logging.getLogger("hamilton.plugins.h_diskcache")
logger.setLevel(logging.DEBUG)  # or logging.INFO
logger.addHandler(logging.StreamHandler())
```
- INFO will only return the total cache after executing the Driver
- DEBUG will return inputs for each node and specify if the value is `from cache` or `executed`

## Clear cache
The utility function `h_diskcache.evict_except_driver` allows you to clear cached values for all nodes except those in the passed driver. This is an efficient tool to clear old artifacts as your project evolves.

```python
from hamilton import driver
from hamilton.plugins import h_diskcache
import functions

dr = (
    driver.Builder()
    .with_modules(functions)
    .with_adapters(h_diskcache.CacheHook())
    .build()
)
h_diskcache_evict_except_driver(dr)
```

## Cache settings
Find all the cache settings in the [diskcache docs](https://grantjenks.com/docs/diskcache/api.html#constants).
